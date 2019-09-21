package message_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestRouter_functional(t *testing.T) {
	testID := watermill.NewUUID()
	subscribeTopic := "test_topic_" + testID

	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()

	messagesCount := 50

	var expectedReceivedMessages message.Messages
	allMessagesSent := make(chan struct{})
	go func() {
		expectedReceivedMessages = publishMessagesForHandler(t, messagesCount, pub, sub, subscribeTopic)
		allMessagesSent <- struct{}{}
	}()

	receivedMessagesCh1 := make(chan *message.Message, messagesCount)
	receivedMessagesCh2 := make(chan *message.Message, messagesCount)
	sentByHandlerCh := make(chan *message.Message, messagesCount)

	publishedEventsTopic := "published_events_" + testID
	publishedByHandlerCh, err := sub.Subscribe(context.Background(), publishedEventsTopic)

	var publishedByHandler message.Messages
	allPublishedByHandler := make(chan struct{})

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
		sub,
		publishedEventsTopic,
		pub,
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
		sub,
		func(msg *message.Message) error {
			receivedMessagesCh2 <- msg
			return nil
		},
	)

	go func() {
		require.NoError(t, r.Run(context.Background()))
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
	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
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
		sub,
		func(msg *message.Message) error {
			messageReceived <- msg

			if !internal.IsChannelClosed(nackSend) {
				msg.Nack()
				close(nackSend)
			}

			return nil
		},
	)

	go func() {
		require.NoError(t, r.Run(context.Background()))
	}()
	defer func() {
		assert.NoError(t, r.Close())
	}()

	<-r.Running()

	publishedMsg := message.NewMessage("1", nil)
	require.NoError(t, pub.Publish("subscribe_topic", publishedMsg))

	messages, all := subscriber.BulkRead(messageReceived, 2, time.Second)
	assert.True(t, all, "not all messages received, probably not ack received, received %d", len(messages))

	tests.AssertAllMessagesReceived(t, []*message.Message{publishedMsg, publishedMsg}, messages)
}

func TestRouter_ack_nack_on_failures(t *testing.T) {
	testCases := []struct {
		Name                 string
		PublisherShouldPanic bool
		PublisherShouldError bool
		HandlerShouldError   bool
		ExpectAck            bool
		ExpectNack           bool
	}{
		{
			Name:                 "publisher_success",
			PublisherShouldPanic: false,
			PublisherShouldError: false,
			HandlerShouldError:   false,
			ExpectAck:            true,
			ExpectNack:           false,
		},
		{
			Name:                 "publisher_error",
			PublisherShouldPanic: false,
			PublisherShouldError: true,
			HandlerShouldError:   false,
			ExpectAck:            false,
			ExpectNack:           true,
		},
		{
			Name:                 "publisher_panic",
			PublisherShouldPanic: true,
			PublisherShouldError: false,
			HandlerShouldError:   false,
			ExpectAck:            false,
			ExpectNack:           true,
		},
		{
			Name:                 "handler_error",
			PublisherShouldPanic: false,
			PublisherShouldError: false,
			HandlerShouldError:   true,
			ExpectAck:            false,
			ExpectNack:           true,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].Name, func(t *testing.T) {
			tc := testCases[i]
			publisher := &failingPublisherMock{
				shouldPanic: tc.PublisherShouldPanic,
				shouldError: tc.PublisherShouldError,
			}
			subscriber := &subscriberMock{
				messages: make(chan *message.Message),
			}
			router, err := message.NewRouter(message.RouterConfig{
				CloseTimeout: time.Second,
			}, watermill.NewStdLogger(true, true))
			require.NoError(t, err)

			handlerMutex := &sync.Mutex{}
			handlerErrored := false

			handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
				handlerMutex.Lock()
				defer handlerMutex.Unlock()
				if tc.HandlerShouldError && !handlerErrored {
					handlerErrored = true
					return nil, errors.New("handler error")
				}
				return message.Messages{msg}, nil
			}

			topic := "topic" + tc.Name
			router.AddHandler(
				"handler"+tc.Name,
				topic,
				subscriber,
				topic,
				publisher,
				handlerFunc,
			)
			go func() {
				err := router.Run(context.Background())
				require.NoError(t, err)
			}()
			<-router.Running()

			msg := message.NewMessage("msg"+tc.Name, []byte{})
			subscriber.messages <- msg

			select {
			case <-msg.Acked():
				assert.True(t, tc.ExpectAck, "did not expect message to be acked")
			case <-msg.Nacked():
				assert.True(t, tc.ExpectNack, "did not expect message to be nacked")
			case <-time.After(5 * time.Second):
				t.Fatal("expected the message to be acked or nacked")
			}

			err = router.Close()
			require.NoError(t, err)
		})
	}
}

type subscriberMock struct {
	messages chan *message.Message
}

func (s *subscriberMock) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return s.messages, nil
}

func (s *subscriberMock) Close() error {
	close(s.messages)
	return nil
}

type failingPublisherMock struct {
	shouldPanic bool
	shouldError bool
}

func (p *failingPublisherMock) Publish(topic string, messages ...*message.Message) error {
	if p.shouldPanic {
		panic("publisher panicked")
	}
	if p.shouldError {
		return errors.New("publisher failed")
	}
	return nil
}

func (p *failingPublisherMock) Close() error { return nil }

func TestRouter_stop_when_all_handlers_stopped(t *testing.T) {
	pub1, sub1 := createPubSub()
	pub2, sub2 := createPubSub()

	defer func() {
		assert.NoError(t, pub1.Close())
		assert.NoError(t, sub1.Close())
		assert.NoError(t, pub2.Close())
		assert.NoError(t, sub2.Close())
	}()

	r, err := message.NewRouter(
		message.RouterConfig{},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	r.AddNoPublisherHandler(
		"handler_1",
		"foo",
		sub1,
		func(msg *message.Message) error {
			return nil
		},
	)

	r.AddNoPublisherHandler(
		"handler_2",
		"foo",
		sub2,
		func(msg *message.Message) error {
			return nil
		},
	)

	routerStopped := make(chan struct{})
	go func() {
		assert.NoError(t, r.Run(context.Background()))
		close(routerStopped)
	}()
	<-r.Running()

	require.NoError(t, pub1.Close())
	require.NoError(t, sub1.Close())
	select {
	case <-routerStopped:
		t.Fatal("only one handler has stopped")
	case <-time.After(time.Millisecond * 100):
		// ok
	}

	require.NoError(t, pub2.Close())
	require.NoError(t, sub2.Close())
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
	if err := router.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func TestRouterNoPublisherHandler(t *testing.T) {
	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()

	logger := watermill.NewCaptureLogger()

	r, err := message.NewRouter(
		message.RouterConfig{},
		logger,
	)
	require.NoError(t, err)

	wait := make(chan struct{})

	r.AddNoPublisherHandler(
		"test_no_publisher_handler",
		"subscribe_topic",
		sub,
		func(msg *message.Message) error {
			close(wait)
			return nil
		},
	)

	go func() {
		err = r.Run(context.Background())
		require.NoError(t, err)
	}()
	defer r.Close()

	<-r.Running()

	publishedMsg := message.NewMessage("1", nil)
	err = pub.Publish("subscribe_topic", publishedMsg)
	require.NoError(t, err)

	select {
	case <-wait:
	// ok
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}

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
		func(msg *message.Message) (e error) {
			allProcessedWg.Done()
			return nil
		},
	)

	go func() {
		allProcessedWg.Wait()
		router.Close()
	}()

	b.ResetTimer()
	if err := router.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
}

// TestRouterDecoratorsOrder checks that the publisher/subscriber decorators are applied in the order they are registered.
func TestRouterDecoratorsOrder(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	pub, sub := createPubSub()

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
		sub,
		"pubTopic",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			return message.Messages{msg}, nil
		},
	)

	go func() {
		if err := router.Run(context.Background()); err != nil {
			panic(err)
		}
	}()
	defer func() {
		if err := router.Close(); err != nil {
			panic(err)
		}
	}()
	<-router.Running()

	transformedMessages, err := sub.Subscribe(context.Background(), "pubTopic")
	require.NoError(t, err)

	var transformedMessage *message.Message
	messageObtained := make(chan struct{})
	go func() {
		transformedMessage = <-transformedMessages
		close(messageObtained)
	}()

	require.NoError(t, pub.Publish("subTopic", message.NewMessage(watermill.NewUUID(), []byte{})))

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out")
	case <-messageObtained:
	}

	assert.Equal(t, "foobar", transformedMessage.Metadata.Get("pub"))
	assert.Equal(t, "foobar", transformedMessage.Metadata.Get("sub"))
}

func TestRouter_concurrent_close(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	go func() {
		err := router.Close()
		require.NoError(t, err)
	}()

	err = router.Close()
	require.NoError(t, err)
}

func TestRouter_concurrent_close_on_handlers_closed(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	_, sub := createPubSub()

	router.AddNoPublisherHandler(
		"handler",
		"subTopic",
		sub,
		func(msg *message.Message) error {
			return nil
		},
	)

	go func() {
		if err := router.Run(context.Background()); err != nil {
			panic(err)
		}
	}()
	<-router.Running()

	go func() {
		err := sub.Close()
		require.NoError(t, err)
	}()

	err = router.Close()
	require.NoError(t, err)
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

func publishMessagesForHandler(t *testing.T, messagesCount int, pub message.Publisher, sub message.Subscriber, topicName string) []*message.Message {
	var messagesToPublish []*message.Message

	for i := 0; i < messagesCount; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("%d", i)))

		messagesToPublish = append(messagesToPublish, msg)
	}

	for _, msg := range messagesToPublish {
		err := pub.Publish(topicName, msg)
		require.NoError(t, err)
	}

	return messagesToPublish
}

func createPubSub() (message.Publisher, message.Subscriber) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{Persistent: true},
		watermill.NewStdLogger(true, true),
	)
	return pubSub, pubSub
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
