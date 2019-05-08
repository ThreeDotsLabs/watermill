package message_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/internal"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_functional(t *testing.T) {
	testID := watermill.NewUUID()
	subscribeTopic := "test_topic_" + testID

	pubSub := createPubSub()
	defer func() {
		assert.NoError(t, pubSub.Close())
	}()

	messagesCount := 50

	var expectedReceivedMessages message.Messages
	allMessagesSent := make(chan struct{})
	go func() {
		expectedReceivedMessages = publishMessagesForHandler(t, messagesCount, pubSub, subscribeTopic)
		allMessagesSent <- struct{}{}
	}()

	receivedMessagesCh1 := make(chan *message.Message, messagesCount)
	receivedMessagesCh2 := make(chan *message.Message, messagesCount)
	sentByHandlerCh := make(chan *message.Message, messagesCount)

	publishedEventsTopic := "published_events_" + testID
	publishedByHandlerCh, err := pubSub.Subscribe(context.Background(), publishedEventsTopic)

	var publishedByHandler message.Messages
	allPublishedByHandler := make(chan struct{}, 0)

	go func() {
		var all bool
		publishedByHandler, all = subscriber.BulkRead(publishedByHandlerCh, messagesCount, time.Second*10)
		assert.True(t, all)
		allPublishedByHandler <- struct{}{}
	}()

	require.NoError(t, err)

	r, err := message.NewRouter(
		message.RouterConfig{},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	r.AddHandler(
		"test_subscriber_1",
		subscribeTopic,
		pubSub,
		publishedEventsTopic,
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			receivedMessagesCh1 <- msg

			toPublish := message.NewMessage(watermill.NewUUID(), nil)
			sentByHandlerCh <- toPublish

			return []*message.Message{toPublish}, nil
		},
	)

	r.AddNoPublisherHandler(
		"test_subscriber_2",
		subscribeTopic,
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			receivedMessagesCh2 <- msg
			return nil, nil
		},
	)

	go func() {
		require.NoError(t, r.Run())
	}()
	<-r.Running()

	defer func() {
		assert.NoError(t, r.Close())
	}()

	<-allMessagesSent

	expectedSentByHandler, all := readMessages(sentByHandlerCh, len(expectedReceivedMessages), time.Second*10)
	assert.True(t, all)

	receivedMessages1, all := subscriber.BulkRead(receivedMessagesCh1, len(expectedReceivedMessages), time.Second*10)
	assert.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages1)

	receivedMessages2, all := subscriber.BulkRead(receivedMessagesCh2, len(expectedReceivedMessages), time.Second*10)
	assert.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages2)

	<-allPublishedByHandler
	tests.AssertAllMessagesReceived(t, expectedSentByHandler, publishedByHandler)
}

func TestRouter_functional_nack(t *testing.T) {
	pubSub := createPubSub()
	defer func() {
		assert.NoError(t, pubSub.Close())
	}()

	r, err := message.NewRouter(
		message.RouterConfig{},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	nackSend := make(chan struct{})
	messageReceived := make(chan *message.Message, 2)

	r.AddNoPublisherHandler(
		"test_subscriber_1",
		"subscribe_topic",
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			messageReceived <- msg

			if !internal.IsChannelClosed(nackSend) {
				msg.Nack()
				close(nackSend)
			}

			return nil, nil
		},
	)

	go func() {
		require.NoError(t, r.Run())
	}()
	defer func() {
		assert.NoError(t, r.Close())
	}()

	<-r.Running()

	publishedMsg := message.NewMessage("1", nil)
	require.NoError(t, pubSub.Publish("subscribe_topic", publishedMsg))

	messages, all := subscriber.BulkRead(messageReceived, 2, time.Second)
	assert.True(t, all, "not all messages received, probably not ack received, received %d", len(messages))

	tests.AssertAllMessagesReceived(t, []*message.Message{publishedMsg, publishedMsg}, messages)
}

func TestRouter_stop_when_all_handlers_stopped(t *testing.T) {
	pubSub1 := createPubSub()
	pubSub2 := createPubSub()

	defer func() {
		assert.NoError(t, pubSub1.Close())
		assert.NoError(t, pubSub2.Close())
	}()

	r, err := message.NewRouter(
		message.RouterConfig{},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	r.AddNoPublisherHandler(
		"handler_1",
		"foo",
		pubSub1,
		func(msg *message.Message) (messages []*message.Message, e error) {
			return nil, nil
		},
	)

	r.AddNoPublisherHandler(
		"handler_2",
		"foo",
		pubSub2,
		func(msg *message.Message) (messages []*message.Message, e error) {
			return nil, nil
		},
	)

	routerStopped := make(chan struct{})
	go func() {
		assert.NoError(t, r.Run())
		close(routerStopped)
	}()
	<-r.Running()

	require.NoError(t, pubSub1.Close())
	select {
	case <-routerStopped:
		t.Fatal("only one handler has stopped")
	case <-time.After(time.Millisecond * 100):
		// ok
	}

	require.NoError(t, pubSub2.Close())
	select {
	case <-routerStopped:
	// ok
	case <-time.After(time.Second):
		t.Fatal("router not closed")
	}
}

type benchMockSubscriber struct {
	messagesToSend []*message.Message
}

func (m benchMockSubscriber) Subscribe(_ context.Context, topic string) (<-chan *message.Message, error) {
	out := make(chan *message.Message)

	go func() {
		for _, msg := range m.messagesToSend {
			out <- msg
			<-msg.Acked()
		}

		close(out)
	}()

	return out, nil
}

func (benchMockSubscriber) Close() error {
	return nil
}

type nopPublisher struct{}

func (nopPublisher) Publish(topic string, messages ...*message.Message) error { return nil }
func (nopPublisher) Close() error                                             { return nil }

func BenchmarkRouterHandler(b *testing.B) {
	logger := watermill.NopLogger{}

	allProcessedWg := sync.WaitGroup{}
	allProcessedWg.Add(b.N)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		b.Fatal(err)
	}

	sub := createBenchSubscriber(b)

	router.AddHandler(
		"handler",
		"benchmark_topic",
		sub,
		"publish_topic",
		nopPublisher{},
		func(msg *message.Message) (messages []*message.Message, e error) {
			allProcessedWg.Done()
			return []*message.Message{msg}, nil
		},
	)

	go func() {
		allProcessedWg.Wait()
		router.Close()
	}()

	b.ResetTimer()
	if err := router.Run(); err != nil {
		b.Fatal(err)
	}
}

func TestRouterNoPublisherHandler(t *testing.T) {
	pubSub := createPubSub()
	defer func() {
		assert.NoError(t, pubSub.Close())
	}()

	logger := watermill.NewCaptureLogger()

	r, err := message.NewRouter(
		message.RouterConfig{},
		logger,
	)
	require.NoError(t, err)

	msgReceived := false
	wait := make(chan struct{})

	r.AddNoPublisherHandler(
		"test_no_publisher_handler",
		"subscribe_topic",
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			if msgReceived {
				require.True(t, msg.Ack())
				close(wait)
				return nil, nil
			}
			msgReceived = true
			return message.Messages{msg}, nil
		},
	)

	go r.Run()
	defer r.Close()

	<-r.Running()

	publishedMsg := message.NewMessage("1", nil)
	err = pubSub.Publish("subscribe_topic", publishedMsg)
	require.NoError(t, err)

	<-wait

	// handler has no publisher, so the router should complain about it
	// however, it returns no error for now (because of how messages are processed in the router),
	// so let's just look for the error in the logger.
	assert.True(t, logger.HasError(message.ErrOutputInNoPublisherHandler))
	require.NoError(t, r.Close())
}

func BenchmarkRouterNoPublisherHandler(b *testing.B) {
	logger := watermill.NopLogger{}

	allProcessedWg := sync.WaitGroup{}
	allProcessedWg.Add(b.N)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		b.Fatal(err)
	}

	sub := createBenchSubscriber(b)

	router.AddNoPublisherHandler(
		"handler",
		"benchmark_topic",
		sub,
		func(msg *message.Message) (messages []*message.Message, e error) {
			allProcessedWg.Done()
			return nil, nil
		},
	)

	go func() {
		allProcessedWg.Wait()
		router.Close()
	}()

	b.ResetTimer()
	if err := router.Run(); err != nil {
		b.Fatal(err)
	}
}

// TestRouterDecoratorsOrder checks that the publisher/subscriber decorators are applied in the order they are registered.
func TestRouterDecoratorsOrder(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	pubSub := createPubSub()

	pubDecorator1 := message.MessageTransformPublisherDecorator(func(m *message.Message) {
		m.Metadata.Set("pub", m.Metadata.Get("pub")+"foo")
	})
	pubDecorator2 := message.MessageTransformPublisherDecorator(func(m *message.Message) {
		m.Metadata.Set("pub", m.Metadata.Get("pub")+"bar")
	})

	subDecorator1 := message.MessageTransformSubscriberDecorator(func(m *message.Message) {
		m.Metadata.Set("sub", m.Metadata.Get("sub")+"foo")
	})
	subDecorator2 := message.MessageTransformSubscriberDecorator(func(m *message.Message) {
		m.Metadata.Set("sub", m.Metadata.Get("sub")+"bar")
	})

	router.AddPublisherDecorators(pubDecorator1, pubDecorator2)
	router.AddSubscriberDecorators(subDecorator1, subDecorator2)

	router.AddHandler(
		"handler",
		"subTopic",
		pubSub,
		"pubTopic",
		pubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			return message.Messages{msg}, nil
		},
	)

	go func() {
		if err := router.Run(); err != nil {
			panic(err)
		}
	}()
	defer func() {
		if err := router.Close(); err != nil {
			panic(err)
		}
	}()
	<-router.Running()

	transformedMessages, err := pubSub.Subscribe(context.Background(), "pubTopic")
	require.NoError(t, err)

	var transformedMessage *message.Message
	messageObtained := make(chan struct{})
	go func() {
		transformedMessage = <-transformedMessages
		close(messageObtained)
	}()

	require.NoError(t, pubSub.Publish("subTopic", message.NewMessage(watermill.NewUUID(), []byte{})))

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out")
	case <-messageObtained:
	}

	assert.Equal(t, "foobar", transformedMessage.Metadata.Get("pub"))
	assert.Equal(t, "foobar", transformedMessage.Metadata.Get("sub"))
}

func createBenchSubscriber(b *testing.B) benchMockSubscriber {
	var messagesToSend []*message.Message
	for i := 0; i < b.N; i++ {
		messagesToSend = append(
			messagesToSend,
			message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("%d", i))),
		)
	}

	return benchMockSubscriber{messagesToSend}
}

func publishMessagesForHandler(t *testing.T, messagesCount int, pubSub message.PubSub, topicName string) []*message.Message {
	var messagesToPublish []*message.Message

	for i := 0; i < messagesCount; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("%d", i)))

		messagesToPublish = append(messagesToPublish, msg)
	}

	for _, msg := range messagesToPublish {
		err := pubSub.Publish(topicName, msg)
		require.NoError(t, err)
	}

	return messagesToPublish
}

func createPubSub() message.PubSub {
	return gochannel.NewGoChannel(
		gochannel.Config{Persistent: true},
		watermill.NewStdLogger(true, true),
	)
}

func readMessages(messagesCh <-chan *message.Message, limit int, timeout time.Duration) (receivedMessages []*message.Message, all bool) {
	allMessagesReceived := make(chan struct{}, 1)

	go func() {
		for msg := range messagesCh {
			receivedMessages = append(receivedMessages, msg)

			if len(receivedMessages) == limit {
				allMessagesReceived <- struct{}{}
				break
			}
		}
		// messagesCh closed
		allMessagesReceived <- struct{}{}
	}()

	select {
	case <-allMessagesReceived:
	case <-time.After(timeout):
	}

	return receivedMessages, len(receivedMessages) == limit
}
