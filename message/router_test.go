package message_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_functional(t *testing.T) {
	testID := uuid.NewV4().String()
	subscribeTopic := "test_topic_" + testID

	pubSub, err := createPubSub()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, pubSub.Close())
	}()

	messagesCount := 100

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
	publishedByHandlerCh, err := pubSub.Subscribe(publishedEventsTopic)

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

	err = r.AddHandler(
		"test_subscriber_1",
		subscribeTopic,
		pubSub,
		publishedEventsTopic,
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			receivedMessagesCh1 <- msg

			toPublish := message.NewMessage(uuid.NewV4().String(), nil)
			sentByHandlerCh <- toPublish

			return []*message.Message{toPublish}, nil
		},
	)
	require.NoError(t, err)

	err = r.AddNoPublisherHandler(
		"test_subscriber_2",
		subscribeTopic,
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			receivedMessagesCh2 <- msg
			return nil, nil
		},
	)
	require.NoError(t, err)

	go r.Run()
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
	pubSub, err := createPubSub()
	require.NoError(t, err)
	defer pubSub.Close()

	r, err := message.NewRouter(
		message.RouterConfig{},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	nackSend := false
	messageReceived := make(chan *message.Message, 2)

	err = r.AddNoPublisherHandler(
		"test_subscriber_1",
		"subscribe_topic",
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			messageReceived <- msg

			if !nackSend {
				msg.Nack()
				nackSend = true
			}

			return nil, nil
		},
	)
	require.NoError(t, err)

	go r.Run()
	defer r.Close()

	<-r.Running()

	publishedMsg := message.NewMessage("1", nil)
	require.NoError(t, pubSub.Publish("subscribe_topic", publishedMsg))

	messages, all := subscriber.BulkRead(messageReceived, 2, time.Second)
	assert.True(t, all, "not all messages received, probably not ack received, received %d", len(messages))

	tests.AssertAllMessagesReceived(t, []*message.Message{publishedMsg, publishedMsg}, messages)
}

type benchMockSubscriber struct {
	messagesToSend []*message.Message
}

func (m benchMockSubscriber) Subscribe(topic string) (chan *message.Message, error) {
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

type nopPublisher struct {
}

func (nopPublisher) Publish(topic string, messages ...*message.Message) error {
	return nil
}

func (nopPublisher) Close() error {
	return nil
}

func BenchmarkRouterHandler(b *testing.B) {
	logger := watermill.NopLogger{}

	allProcessedWg := sync.WaitGroup{}
	allProcessedWg.Add(b.N)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		b.Fatal(err)
	}

	sub := createBenchSubscriber(b)

	if err := router.AddHandler(
		"handler",
		"benchmark_topic",
		sub,
		"publish_topic",
		nopPublisher{},
		func(msg *message.Message) (messages []*message.Message, e error) {
			allProcessedWg.Done()
			return []*message.Message{msg}, nil
		},
	); err != nil {
		b.Fatal(err)
	}

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
	pubSub, err := createPubSub()
	require.NoError(t, err)
	defer pubSub.Close()

	logger := watermill.NewCaptureLogger()

	r, err := message.NewRouter(
		message.RouterConfig{},
		&logger,
	)
	require.NoError(t, err)

	msgReceived := false
	wait := make(chan struct{})

	err = r.AddNoPublisherHandler(
		"test_no_publisher_handler",
		"subscribe_topic",
		pubSub,
		func(msg *message.Message) (producedMessages []*message.Message, err error) {
			if msgReceived {
				require.NoError(t, msg.Ack())
				close(wait)
				return nil, nil
			}
			msgReceived = true
			return message.Messages{msg}, nil
		},
	)
	require.NoError(t, err)

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

	if err := router.AddNoPublisherHandler(
		"handler",
		"benchmark_topic",
		sub,
		func(msg *message.Message) (messages []*message.Message, e error) {
			allProcessedWg.Done()
			return nil, nil
		},
	); err != nil {
		b.Fatal(err)
	}

	go func() {
		allProcessedWg.Wait()
		router.Close()
	}()

	b.ResetTimer()
	if err := router.Run(); err != nil {
		b.Fatal(err)
	}
}

func createBenchSubscriber(b *testing.B) benchMockSubscriber {
	var messagesToSend []*message.Message
	for i := 0; i < b.N; i++ {
		messagesToSend = append(
			messagesToSend,
			message.NewMessage(uuid.NewV4().String(), []byte(fmt.Sprintf("%d", i))),
		)
	}

	return benchMockSubscriber{messagesToSend}
}

func publishMessagesForHandler(t *testing.T, messagesCount int, pubSub message.PubSub, topicName string) []*message.Message {
	var messagesToPublish []*message.Message

	for i := 0; i < messagesCount; i++ {
		msg := message.NewMessage(uuid.NewV4().String(), []byte(fmt.Sprintf("%d", i)))

		messagesToPublish = append(messagesToPublish, msg)
	}

	for _, msg := range messagesToPublish {
		err := pubSub.Publish(topicName, msg)
		require.NoError(t, err)
	}

	return messagesToPublish
}

func createPubSub() (message.PubSub, error) {
	return gochannel.NewGoChannel(0, watermill.NewStdLogger(true, true), time.Second*10), nil
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
