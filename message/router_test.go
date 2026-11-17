package message_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
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

	r.AddConsumerHandler(
		"test_subscriber_2",
		subscribeTopic,
		sub,
		func(msg *message.Message) error {
			receivedMessagesCh2 <- msg
			return nil
		},
	)

	go func() {
		assert.False(t, r.IsRunning())
		assert.NoError(t, r.Run(context.Background()))
	}()
	<-r.Running()

	defer func() {
		assert.True(t, r.IsRunning())
		assert.NoError(t, r.Close())

		assert.True(t, r.IsClosed())
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

	r.AddConsumerHandler(
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

func TestRouter_ack_on_publishing_success(t *testing.T) {
	publisher := &failingPublisherMock{
		shouldPanic: false,
		shouldError: false,
	}
	subscriber := &subscriberMock{
		messages: make(chan *message.Message),
	}
	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second,
	}, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		return message.Messages{msg}, nil
	}

	topic := "ack_on_publishing_success"
	router.AddHandler(
		"ack_on_publishing_success_handler",
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

	msg := message.NewMessage("uuid", []byte{})
	subscriber.messages <- msg

	select {
	case <-msg.Acked():
		// ok
	case <-msg.Nacked():
		t.Fatal("did not expect the message to be nacked")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the message to be acked")
	}

	err = router.Close()
	require.NoError(t, err)
}

func TestRouter_nack_on_publishing_failure(t *testing.T) {
	publisher := &failingPublisherMock{
		shouldPanic: false,
		shouldError: true,
	}
	subscriber := &subscriberMock{
		messages: make(chan *message.Message),
	}
	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second,
	}, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		return message.Messages{msg}, nil
	}

	topic := "nack_on_publishing_failure"
	router.AddHandler(
		"nack_on_publishing_failure_handler",
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

	msg := message.NewMessage("uuid", []byte{})
	subscriber.messages <- msg

	select {
	case <-msg.Acked():
		t.Fatal("did not expect the message to be acked")
	case <-msg.Nacked():
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("expected the message to be nacked")
	}

	err = router.Close()
	require.NoError(t, err)
}

func TestRouter_nack_on_panic(t *testing.T) {
	publisher := &failingPublisherMock{
		shouldPanic: true,
		shouldError: false,
	}
	subscriber := &subscriberMock{
		messages: make(chan *message.Message),
	}
	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second,
	}, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		return message.Messages{msg}, nil
	}

	topic := "nack_on_panic"
	router.AddHandler(
		"nack_on_panic_handler",
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

	msg := message.NewMessage("uuid", []byte{})
	subscriber.messages <- msg

	select {
	case <-msg.Acked():
		t.Fatal("did not expect the message to be acked")
	case <-msg.Nacked():
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("expected the message to be nacked")
	}

	err = router.Close()
	require.NoError(t, err)
}

func TestRouter_nack_on_handler_failure(t *testing.T) {
	publisher := &failingPublisherMock{
		shouldPanic: false,
		shouldError: false,
	}
	subscriber := &subscriberMock{
		messages: make(chan *message.Message),
	}
	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second,
	}, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		return nil, errors.New("handler error")
	}

	topic := "nack_on_handler_failure"
	router.AddHandler(
		"nack_on_handler_failure_handler",
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

	msg := message.NewMessage("uuid", []byte{})
	subscriber.messages <- msg

	select {
	case <-msg.Acked():
		t.Fatal("did not expect the message to be acked")
	case <-msg.Nacked():
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("expected the message to be nacked")
	}
}

func TestRouter_AddMiddleware_to_router(t *testing.T) {
	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()
	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second,
	}, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	middlewareCount := 3
	middlewareCh := make(chan string, middlewareCount)
	allMiddlewareExecuted := make(chan struct{}, 1)
	var executedMiddleware []string

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		return message.Messages{msg}, nil
	}
	firstMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "firstMiddleware"
		return h
	}

	secondMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "secondMiddleware"
		return h
	}

	thirdMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "thirdMiddleware"
		return h
	}

	topic := "some_topic"
	router.AddMiddleware(firstMiddleware)
	router.AddHandler(
		"some_topic_handler",
		topic,
		sub,
		topic,
		pub,
		handlerFunc,
	)
	router.AddMiddleware(secondMiddleware)
	router.AddMiddleware(thirdMiddleware)

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		msg := message.NewMessage(watermill.NewUUID(), []byte("test_payload"))
		err := pub.Publish(topic, msg)
		require.NoError(t, err)
	}()

	go func() {
		for middlewareName := range middlewareCh {
			executedMiddleware = append(executedMiddleware, middlewareName)

			if len(executedMiddleware) == 3 {
				allMiddlewareExecuted <- struct{}{}
				break
			}
		}
		allMiddlewareExecuted <- struct{}{}
	}()

	go func() {
		err := router.Run(context.Background())
		require.NoError(t, err)
	}()
	<-router.Running()
	<-allMiddlewareExecuted
	<-publishDone

	err = router.Close()
	require.NoError(t, err)

	require.Equal(t, "firstMiddleware", executedMiddleware[2])
	require.Equal(t, "secondMiddleware", executedMiddleware[1])
	require.Equal(t, "thirdMiddleware", executedMiddleware[0])
}

func TestRouter_AddMiddleware_to_handler(t *testing.T) {
	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()
	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second,
	}, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	middlewareCount := 4
	middlewareCh := make(chan string, middlewareCount)
	allMiddlewareExecuted := make(chan struct{}, 1)
	var executedMiddleware []string

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		return message.Messages{msg}, nil
	}
	firstMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "firstMiddleware"
		return h
	}

	secondMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "secondMiddleware"
		return h
	}

	thirdMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "thirdMiddleware"
		return h
	}

	fourthMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "fourthMiddleware"
		return h
	}

	topic := "some_topic"
	router.AddMiddleware(firstMiddleware)
	router.AddHandler(
		"some_topic_handler",
		topic,
		sub,
		topic,
		pub,
		handlerFunc,
	).AddMiddleware(secondMiddleware, thirdMiddleware)
	router.AddMiddleware(fourthMiddleware)

	go func() {
		for middlewareName := range middlewareCh {
			executedMiddleware = append(executedMiddleware, middlewareName)

			if len(executedMiddleware) == middlewareCount {
				allMiddlewareExecuted <- struct{}{}
				break
			}
		}
		allMiddlewareExecuted <- struct{}{}
	}()

	go func() {
		err := router.Run(context.Background())
		require.NoError(t, err)
	}()
	<-router.Running()
	<-allMiddlewareExecuted

	err = router.Close()
	require.NoError(t, err)

	require.Equal(t, "firstMiddleware", executedMiddleware[3])
	require.Equal(t, "secondMiddleware", executedMiddleware[2])
	require.Equal(t, "thirdMiddleware", executedMiddleware[1])
	require.Equal(t, "fourthMiddleware", executedMiddleware[0])
}

func TestRouter_AddMiddleware_to_handler_many(t *testing.T) {
	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()
	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second,
	}, watermill.NewStdLogger(true, true))
	require.NoError(t, err)

	middlewareCount := 6
	middlewareCh := make(chan string, middlewareCount)
	allMiddlewareExecuted := make(chan struct{}, 1)
	var executedMiddleware []string

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		return message.Messages{msg}, nil
	}
	firstMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "firstMiddleware"
		return h
	}

	secondMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "secondMiddleware"
		return h
	}

	thirdMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "thirdMiddleware"
		return h
	}

	fourthMiddleware := func(h message.HandlerFunc) message.HandlerFunc {
		middlewareCh <- "fourthMiddleware"
		return h
	}

	topic := "some_topic"
	router.AddMiddleware(firstMiddleware)
	router.AddHandler(
		"some_topic_handler",
		topic,
		sub,
		topic,
		pub,
		handlerFunc,
	).AddMiddleware(secondMiddleware)
	router.AddHandler(
		"some_other_topic_handler",
		"some_other_topic",
		sub,
		"some_other_topic",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			return message.Messages{msg}, nil
		},
	).AddMiddleware(thirdMiddleware)
	router.AddMiddleware(fourthMiddleware)

	go func() {
		for middlewareName := range middlewareCh {
			executedMiddleware = append(executedMiddleware, middlewareName)

			if len(executedMiddleware) == middlewareCount {
				allMiddlewareExecuted <- struct{}{}
				break
			}
		}
		allMiddlewareExecuted <- struct{}{}
	}()

	go func() {
		err := router.Run(context.Background())
		require.NoError(t, err)
	}()
	<-router.Running()
	<-allMiddlewareExecuted

	err = router.Close()
	require.NoError(t, err)

	require.Equal(t, 6, len(executedMiddleware))
	counts := map[string]int{}
	for _, m := range executedMiddleware {
		counts[m] += 1
	}
	require.Equal(t, 2, counts["firstMiddleware"])
	require.Equal(t, 1, counts["secondMiddleware"])
	require.Equal(t, 1, counts["thirdMiddleware"])
	require.Equal(t, 2, counts["fourthMiddleware"])
}

func TestRouter_RunHandlers(t *testing.T) {
	ctx := context.Background()

	testID := watermill.NewUUID()
	subscribeTopic := "test_topic_" + testID

	pubsub := gochannel.NewGoChannel(
		gochannel.Config{Persistent: true},
		watermill.NewStdLogger(true, true),
	)
	defer func() {
		assert.NoError(t, pubsub.Close())
	}()

	r, err := message.NewRouter(
		message.RouterConfig{},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, r.Close())
	}()

	go func() {
		require.NoError(t, r.Run(ctx))
	}()
	<-r.Running()

	messagesCount := 3

	var expectedReceivedMessages message.Messages

	receivedMessagesCh := make(chan *message.Message, messagesCount)

	handler := r.AddConsumerHandler(
		"test_subscriber_1",
		subscribeTopic,
		pubsub,
		func(msg *message.Message) error {
			receivedMessagesCh <- msg
			return nil
		},
	)
	require.NotNil(t, handler)
	require.NoError(t, err)

	require.NoError(t, r.RunHandlers(ctx))
	require.NoError(t, r.RunHandlers(ctx)) // RunHandlers should be idempotent

	select {
	case <-handler.Started():
	// ok
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for handler")
	}

	expectedReceivedMessages = publishMessagesForHandler(t, messagesCount, pubsub, pubsub, subscribeTopic)

	receivedMessages1, all := subscriber.BulkRead(receivedMessagesCh, len(expectedReceivedMessages), time.Second*10)
	assert.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages1)
}

func TestRouter_close_handler(t *testing.T) {
	testID := watermill.NewUUID()
	subscribeTopic1 := "test_topic_1_" + testID
	subscribeTopic2 := "test_topic_2_" + testID

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

	messagesCount := 3
	var expectedReceivedMessages message.Messages
	receivedMessagesCh1 := make(chan *message.Message, messagesCount)

	handler := r.AddConsumerHandler(
		"test_subscriber_1",
		subscribeTopic1,
		sub,
		func(msg *message.Message) error {
			receivedMessagesCh1 <- msg
			return nil
		},
	)

	// to keep at least one running handler to prevent router from closing
	r.AddConsumerHandler(
		"noop_handler",
		watermill.NewUUID(),
		sub,
		func(msg *message.Message) error {
			return nil
		},
	)

	go func() {
		require.NoError(t, r.Run(context.Background()))
	}()
	<-r.Running()

	expectedReceivedMessages = publishMessagesForHandler(t, messagesCount, pub, sub, subscribeTopic1)
	receivedMessages1, all := subscriber.BulkRead(receivedMessagesCh1, len(expectedReceivedMessages), time.Second*10)
	assert.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages1)

	handler.Stop()
	select {
	case <-handler.Stopped():
	// ok
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for handler stopped")
	}

	_ = publishMessagesForHandler(t, 1, pub, sub, subscribeTopic1)
	_, received := subscriber.BulkRead(receivedMessagesCh1, 1, time.Millisecond*1)
	assert.False(t, received)

	receivedMessagesCh2 := make(chan *message.Message, messagesCount)

	// we are adding the same handler again, with the same name
	r.AddConsumerHandler(
		"test_subscriber_1",
		subscribeTopic2,
		sub,
		func(msg *message.Message) error {
			receivedMessagesCh2 <- msg
			return nil
		},
	)
	err = r.RunHandlers(context.Background())
	require.NoError(t, err)

	expectedReceivedMessages = publishMessagesForHandler(t, messagesCount, pub, sub, subscribeTopic2)
	receivedMessages2, all := subscriber.BulkRead(receivedMessagesCh2, len(expectedReceivedMessages), time.Second*10)
	assert.True(t, all)
	tests.AssertAllMessagesReceived(t, expectedReceivedMessages, receivedMessages2)
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

	r.AddConsumerHandler(
		"handler_1",
		"foo",
		sub1,
		func(msg *message.Message) error {
			return nil
		},
	)

	r.AddConsumerHandler(
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
		t.Fatal("router not stopped")
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
		_ = router.Close()
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

	r.AddConsumerHandler(
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
	defer func() { _ = r.Close() }()

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

	router.AddConsumerHandler(
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
		_ = router.Close()
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

	router.AddConsumerHandler(
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
			message.NewMessage(watermill.NewUUID(), fmt.Appendf(nil, "%d", i)),
		)
	}

	return benchMockSubscriber{messagesToSend}
}

func publishMessagesForHandler(t *testing.T, messagesCount int, pub message.Publisher, sub message.Subscriber, topicName string) []*message.Message {
	var messagesToPublish []*message.Message

	for i := 0; i < messagesCount; i++ {
		msg := message.NewMessage(watermill.NewUUID(), fmt.Appendf(nil, "%d", i))

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
		// messagesCh stopped
		allMessagesReceived <- struct{}{}
	}()

	select {
	case <-allMessagesReceived:
	case <-time.After(timeout):
	}

	return receivedMessages, len(receivedMessages) == limit
}

func TestRouter_Handlers(t *testing.T) {
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

	handlerCalled := false

	handlerName := "test_get_handler"

	r.AddConsumerHandler(
		handlerName,
		"subscribe_topic",
		sub,
		func(msg *message.Message) error {
			handlerCalled = true
			return nil
		},
	)

	actual := r.Handlers()

	assert.Len(t, actual, 1)

	actualHandler := actual[handlerName]

	assert.NotNil(t, actualHandler)

	messages, err := actualHandler(nil)

	assert.Empty(t, messages)
	assert.NoError(t, err)
	assert.True(t, handlerCalled, "Handler function should be the same")
}

func TestRouter_wait_for_handlers_before_shutdown(t *testing.T) {
	t.Parallel()

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

	handlerStarted := make(chan struct{})
	routerClosed := make(chan struct{})

	r.AddConsumerHandler(
		"foo",
		"subscribe_topic",
		sub,
		func(msg *message.Message) error {
			close(handlerStarted)
			select {}
		},
	)

	go func() {
		err := r.Run(context.Background())
		assert.NoError(t, err)
	}()
	<-r.Running()

	err = pub.Publish("subscribe_topic", message.NewMessage(watermill.NewUUID(), nil))
	require.NoError(t, err)

	<-handlerStarted

	go func() {
		assert.NoError(t, r.Close())
		close(routerClosed)
	}()

	select {
	case <-routerClosed:
		t.Fatal("Router should wait for handlers to finish")
	case <-time.After(time.Millisecond * 100):
		// ok, router is still running
	}
}

func TestRouter_wait_for_handlers_before_shutdown_timeout(t *testing.T) {
	t.Parallel()

	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()

	logger := watermill.NewCaptureLogger()

	r, err := message.NewRouter(
		message.RouterConfig{
			CloseTimeout: time.Millisecond * 1,
		},
		logger,
	)
	require.NoError(t, err)

	handlerStarted := make(chan struct{})

	r.AddConsumerHandler(
		"foo",
		"subscribe_topic",
		sub,
		func(msg *message.Message) error {
			close(handlerStarted)
			select {}
		},
	)

	go func() {
		err := r.Run(context.Background())
		assert.NoError(t, err)
	}()
	<-r.Running()

	err = pub.Publish("subscribe_topic", message.NewMessage(watermill.NewUUID(), nil))
	require.NoError(t, err)

	<-handlerStarted

	assert.EqualError(t, r.Close(), "router close timeout")
}

func TestRouter_context_cancel_does_not_log_error(t *testing.T) {
	t.Parallel()

	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()

	logger := watermill.NewCaptureLogger()

	r, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	r.AddConsumerHandler(
		"foo",
		"subscribe_topic",
		sub,
		func(msg *message.Message) error {
			return nil
		},
	)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := r.Run(ctx)
		assert.NoError(t, err)
	}()
	<-r.Running()

	// Cancel the context
	cancel()

	require.Eventually(t, func() bool {
		return r.IsClosed()
	}, 3*time.Second, 5*time.Millisecond, "Router should be closed after all handlers are stopped")

	assert.Empty(t, logger.Captured()[watermill.ErrorLogLevel], "No error should be logged when context is canceled")
}

// TestRouter_nack_on_context_canceled checks that the message is Nacked
// when the handler returns context.Canceled.
func TestRouter_nack_on_context_canceled(t *testing.T) {
	t.Parallel()

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})
	defer func() {
		assert.NoError(t, pubSub.Close())
	}()

	logger := watermill.NewStdLogger(false, false) // Use StdLogger, logging check is in another test

	r, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	handlerProcessed := make(chan struct{})
	subscribeTopic := "test_nack_on_context_canceled_" + watermill.NewUUID()

	r.AddConsumerHandler(
		"test_handler",
		subscribeTopic,
		pubSub,
		func(msg *message.Message) error {
			defer func() {
				handlerProcessed <- struct{}{}
			}()
			// Simulate handler returning context.Canceled
			return context.Canceled
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- r.Run(ctx)
	}()
	select {
	case <-r.Running():
		// proceed
	case err := <-runErrCh:
		t.Fatalf("Router failed to start: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Router did not start")
	}

	// Publish a message to trigger the handler
	msg := message.NewMessage(watermill.NewUUID(), nil)
	err = pubSub.Publish(subscribeTopic, msg)
	require.NoError(t, err)

	// Wait for the handler to process the message
	select {
	case <-handlerProcessed:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("Handler did not process message in time")
	}

	// Message should be re-sent when nacked
	select {
	case <-handlerProcessed:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("Handler did not process message in time")
	}
}

func TestRouter_stopping_all_handlers_logs_error(t *testing.T) {
	t.Parallel()

	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()

	logger := watermill.NewCaptureLogger()

	defer logger.PrintCaptured(t)

	r, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	r.AddConsumerHandler(
		"foo",
		"subscribe_topic",
		sub,
		func(msg *message.Message) error {
			return nil
		},
	)

	ctx := context.Background()

	go func() {
		err := r.Run(ctx)
		assert.NoError(t, err)
	}()
	<-r.Running()

	// Stop the subscriber - this should close the router with an error logged
	err = sub.Close()
	require.NoError(t, err)

	require.Eventually(
		t,
		func() bool {
			return r.IsClosed()
		},
		1*time.Second,
		1*time.Millisecond,
		"Router should be closed after all handlers are stopped",
	)

	expectedLogMessage := watermill.CapturedMessage{
		Level: watermill.ErrorLogLevel,
		Msg:   "All handlers stopped, closing router",
		Err:   errors.New("all router handlers stopped"),
	}

	// Note: using logger.Has does not work here, since the error is not exposed (and thus not deep equal-able)
	for _, capturedMessage := range logger.Captured()[watermill.ErrorLogLevel] {
		if capturedMessage.Level == expectedLogMessage.Level &&
			capturedMessage.Msg == expectedLogMessage.Msg &&
			capturedMessage.Err.Error() == expectedLogMessage.Err.Error() {
			return
		}
	}

	assert.Fail(
		t,
		"expected log message not found, logs: %#v",
		logger.Captured(),
	)
}

func TestRouter_stopped_handler_does_not_close_shared_publisher(t *testing.T) {
	testCases := []struct {
		Name        string
		StopHandler func(t *testing.T, handler *message.Handler, subscriber *contextSubscriberMock)
	}{
		{
			Name: "handler_stopped",
			StopHandler: func(t *testing.T, handler *message.Handler, subscriber *contextSubscriberMock) {
				handler.Stop()
			},
		},
		{
			Name: "subscription_closed",
			StopHandler: func(t *testing.T, handler *message.Handler, subscriber *contextSubscriberMock) {
				require.NoError(t, subscriber.Close())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			publisher := newClosablePublisherMock()
			subscriber1 := newContextSubscriberMock()
			subscriber2 := newContextSubscriberMock()

			r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, watermill.NopLogger{})
			require.NoError(t, err)

			handler1 := r.AddHandler("handler_1", "topic_1", subscriber1, "out", publisher, message.PassthroughHandler)
			r.AddHandler("handler_2", "topic_2", subscriber2, "out", publisher, message.PassthroughHandler)

			go func() {
				assert.NoError(t, r.Run(context.Background()))
			}()
			<-r.Running()
			defer func() {
				assert.NoError(t, r.Close())
			}()

			tc.StopHandler(t, handler1, subscriber1)
			waitFor(t, handler1.Stopped(), "handler_1 to stop")

			msg := message.NewMessage(watermill.NewUUID(), nil)
			sendMessage(t, subscriber2, msg)

			// handler_2 is still running, so it must still be able to publish with the shared publisher
			waitForAck(t, msg)
		})
	}
}

func TestRouter_close_closes_publisher_of_stopped_handler(t *testing.T) {
	t.Parallel()

	publisher := newClosablePublisherMock()
	subscriber := newContextSubscriberMock()

	r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, watermill.NopLogger{})
	require.NoError(t, err)

	handler := r.AddHandler("handler", "topic", subscriber, "out", publisher, message.PassthroughHandler)

	// to keep at least one running handler to prevent router from closing
	r.AddConsumerHandler("other_handler", "topic", newContextSubscriberMock(), func(msg *message.Message) error {
		return nil
	})

	go func() {
		assert.NoError(t, r.Run(context.Background()))
	}()
	<-r.Running()

	handler.Stop()
	waitFor(t, handler.Stopped(), "handler to stop")

	require.NoError(t, r.Close())

	assert.True(t, publisher.IsClosed(), "publisher of a stopped handler should be closed when the router is closed")
}

func TestRouter_close_closes_shared_publisher_once_per_handler(t *testing.T) {
	testCases := []struct {
		Name       string
		Decorators []message.PublisherDecorator
	}{
		{
			Name: "without_decorators",
		},
		{
			Name:       "with_decorators",
			Decorators: []message.PublisherDecorator{message.MessageTransformPublisherDecorator(func(*message.Message) {})},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			publisher := newClosablePublisherMock()

			r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, watermill.NopLogger{})
			require.NoError(t, err)
			r.AddPublisherDecorators(tc.Decorators...)

			r.AddHandler("handler_1", "topic", newContextSubscriberMock(), "out", publisher, message.PassthroughHandler)
			r.AddHandler("handler_2", "topic", newContextSubscriberMock(), "out", publisher, message.PassthroughHandler)

			go func() {
				assert.NoError(t, r.Run(context.Background()))
			}()
			<-r.Running()

			require.NoError(t, r.Close())

			assert.EqualValues(t, 2, publisher.closeCalls.Load())
		})
	}
}

func TestRouter_close_closes_non_comparable_publisher(t *testing.T) {
	t.Parallel()

	// a publisher passed by value, which can't be compared, because it has a slice field
	publisher := nonComparablePublisherMock{
		closeCalls: &atomic.Int32{},
		topics:     []string{"out"},
	}

	r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, watermill.NopLogger{})
	require.NoError(t, err)

	r.AddHandler("handler_1", "topic", newContextSubscriberMock(), "out", publisher, message.PassthroughHandler)
	r.AddHandler("handler_2", "topic", newContextSubscriberMock(), "out", publisher, message.PassthroughHandler)

	go func() {
		assert.NoError(t, r.Run(context.Background()))
	}()
	<-r.Running()

	require.NoError(t, r.Close())

	assert.NotZero(t, publisher.closeCalls.Load())
}

func TestRouter_close_publishes_messages_of_running_handlers(t *testing.T) {
	t.Parallel()

	logger := watermill.NewCaptureLogger()
	publisher := newClosablePublisherMock()
	subscriber := newContextSubscriberMock()

	r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, logger)
	require.NoError(t, err)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})

	r.AddHandler(
		"handler",
		"topic",
		subscriber,
		"out",
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			close(handlerStarted)
			<-releaseHandler
			return message.Messages{message.NewMessage(watermill.NewUUID(), nil)}, nil
		},
	)

	go func() {
		assert.NoError(t, r.Run(context.Background()))
	}()
	<-r.Running()

	msg := message.NewMessage(watermill.NewUUID(), nil)
	sendMessage(t, subscriber, msg)
	waitFor(t, handlerStarted, "handler to start processing the message")

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- r.Close()
	}()

	waitForSubscriberStopped(t, logger, "handler", "topic")
	close(releaseHandler)

	// the message is nacked if the publisher was closed before the handler's output was published
	waitForAck(t, msg)

	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for router to close")
	}

	assert.True(t, publisher.IsClosed(), "publisher should be closed when the router is closed")
}

func TestRouter_close_timeout_closes_publishers(t *testing.T) {
	t.Parallel()

	publisher := newClosablePublisherMock()
	subscriber := newContextSubscriberMock()

	r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Millisecond}, watermill.NopLogger{})
	require.NoError(t, err)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	defer close(releaseHandler)

	r.AddHandler(
		"handler",
		"topic",
		subscriber,
		"out",
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			close(handlerStarted)
			<-releaseHandler
			return nil, nil
		},
	)

	go func() {
		assert.NoError(t, r.Run(context.Background()))
	}()
	<-r.Running()

	sendMessage(t, subscriber, message.NewMessage(watermill.NewUUID(), nil))
	waitFor(t, handlerStarted, "handler to start processing the message")

	assert.EqualError(t, r.Close(), "router close timeout")

	waitFor(t, publisher.closed, "publisher to be closed after the close timeout")
}

func TestRouter_handler_stopped_after_running_messages_are_processed(t *testing.T) {
	t.Parallel()

	logger := watermill.NewCaptureLogger()
	subscriber := newContextSubscriberMock()

	r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, logger)
	require.NoError(t, err)

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})
	stoppedWhileProcessing := make(chan struct{})

	var handler *message.Handler
	handler = r.AddConsumerHandler("handler", "topic", subscriber, func(msg *message.Message) error {
		defer close(handlerFinished)

		close(handlerStarted)
		<-releaseHandler

		select {
		case <-handler.Stopped():
			close(stoppedWhileProcessing)
		default:
		}

		return nil
	})

	// to keep at least one running handler to prevent router from closing
	r.AddConsumerHandler("other_handler", "topic", newContextSubscriberMock(), func(msg *message.Message) error {
		return nil
	})

	go func() {
		assert.NoError(t, r.Run(context.Background()))
	}()
	<-r.Running()
	defer func() {
		assert.NoError(t, r.Close())
	}()

	msg := message.NewMessage(watermill.NewUUID(), nil)
	sendMessage(t, subscriber, msg)
	waitFor(t, handlerStarted, "handler to start processing the message")

	handler.Stop()
	waitForSubscriberStopped(t, logger, "handler", "topic")
	close(releaseHandler)

	waitFor(t, handler.Stopped(), "handler to stop")
	waitFor(t, handlerFinished, "handler to finish processing the message")

	select {
	case <-stoppedWhileProcessing:
		t.Fatal("handler was reported as stopped while it was still processing a message")
	default:
	}
	assertAcked(t, msg)
}

// contextSubscriberMock delivers messages sent with sendMessage to a single subscription.
// The subscription is closed when the context passed to Subscribe is canceled or when Close is called.
type contextSubscriberMock struct {
	messages  chan *message.Message
	closing   chan struct{}
	closeOnce sync.Once
}

func newContextSubscriberMock() *contextSubscriberMock {
	return &contextSubscriberMock{
		messages: make(chan *message.Message),
		closing:  make(chan struct{}),
	}
}

func (s *contextSubscriberMock) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)

	go func() {
		defer close(output)

		for {
			select {
			case msg := <-s.messages:
				select {
				case output <- msg:
				case <-ctx.Done():
					return
				case <-s.closing:
					return
				}
			case <-ctx.Done():
				return
			case <-s.closing:
				return
			}
		}
	}()

	return output, nil
}

func (s *contextSubscriberMock) Close() error {
	s.closeOnce.Do(func() {
		close(s.closing)
	})
	return nil
}

func sendMessage(t *testing.T, subscriber *contextSubscriberMock, msg *message.Message) {
	t.Helper()

	select {
	case subscriber.messages <- msg:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscriber to receive the message")
	}
}

// closablePublisherMock fails to publish after it was closed, like real publishers do.
type closablePublisherMock struct {
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func newClosablePublisherMock() *closablePublisherMock {
	return &closablePublisherMock{
		closed: make(chan struct{}),
	}
}

func (p *closablePublisherMock) Publish(topic string, messages ...*message.Message) error {
	if p.IsClosed() {
		return errors.New("publisher closed")
	}
	return nil
}

func (p *closablePublisherMock) Close() error {
	p.closeCalls.Add(1)
	p.closeOnce.Do(func() {
		close(p.closed)
	})
	return nil
}

type nonComparablePublisherMock struct {
	closeCalls *atomic.Int32
	topics     []string
}

func (p nonComparablePublisherMock) Publish(topic string, messages ...*message.Message) error {
	return nil
}

func (p nonComparablePublisherMock) Close() error {
	p.closeCalls.Add(1)
	return nil
}

func (p *closablePublisherMock) IsClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

// waitForSubscriberStopped waits until the router logs that the handler stopped receiving messages.
func waitForSubscriberStopped(t *testing.T, logger *watermill.CaptureLoggerAdapter, handlerName string, topic string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return logger.Has(watermill.CapturedMessage{
			Level:  watermill.InfoLogLevel,
			Fields: watermill.LogFields{"subscriber_name": handlerName, "topic": topic},
			Msg:    "Subscriber stopped",
		})
	}, time.Second, time.Millisecond, "handler %s did not stop receiving messages", handlerName)
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
}

func waitForAck(t *testing.T, msg *message.Message) {
	t.Helper()

	select {
	case <-msg.Acked():
	case <-msg.Nacked():
		t.Fatal("message was nacked")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

func TestRouter_adding_handler_when_last_handler_stops(t *testing.T) {
	t.Parallel()

	for i := 0; i < 10; i++ {
		var r *message.Router
		var addOnce sync.Once
		handlerAdded := make(chan *message.Handler, 1)

		// The handler is added from the stopping handler's goroutine, right after it was counted as stopped,
		// before the router has a chance to notice that its last handler stopped.
		logger := &infoLogHook{
			onInfo: func(msg string) {
				if msg != "Subscriber stopped" {
					return
				}
				addOnce.Do(func() {
					handlerAdded <- r.AddConsumerHandler("handler_2", "topic", newContextSubscriberMock(), func(msg *message.Message) error {
						return nil
					})
				})
			},
		}

		var err error
		r, err = message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, logger)
		require.NoError(t, err)

		handler1 := r.AddConsumerHandler("handler_1", "topic", newContextSubscriberMock(), func(msg *message.Message) error {
			return nil
		})

		runFinished := make(chan struct{})
		go func() {
			defer close(runFinished)
			assert.NoError(t, r.Run(context.Background()))
		}()
		<-r.Running()

		handler1.Stop()

		var handler2 *message.Handler
		select {
		case handler2 = <-handlerAdded:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for handler_2 to be added")
		}

		require.NoError(t, r.RunHandlers(context.Background()))
		handler2.Stop()
		waitFor(t, runFinished, "router to close after all handlers stopped")
	}
}

type infoLogHook struct {
	watermill.NopLogger
	onInfo func(msg string)
}

func (l *infoLogHook) Info(msg string, fields watermill.LogFields) {
	l.onInfo(msg)
}

func (l *infoLogHook) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return l
}

func TestRouter_started_without_handlers_closes_when_added_handler_stops(t *testing.T) {
	t.Parallel()

	for i := 0; i < 10; i++ {
		r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, watermill.NopLogger{})
		require.NoError(t, err)

		runFinished := make(chan struct{})
		go func() {
			defer close(runFinished)
			assert.NoError(t, r.Run(context.Background()))
		}()
		<-r.Running()

		handler := r.AddConsumerHandler("handler", "topic", newContextSubscriberMock(), func(msg *message.Message) error {
			return nil
		})
		require.NoError(t, r.RunHandlers(context.Background()))

		handler.Stop()
		waitFor(t, runFinished, "router to close after its only handler stopped")
	}
}

func TestRouter_close_waits_for_message_received_while_closing(t *testing.T) {
	t.Parallel()

	// The message is received while Close is already waiting, which depends on scheduling,
	// so the scenario is repeated a few times.
	for i := 0; i < 10; i++ {
		logger := watermill.NewCaptureLogger()
		lateMessage := message.NewMessage(watermill.NewUUID(), nil)
		subscriber := newDeliverOnCloseSubscriberMock(lateMessage)

		r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, logger)
		require.NoError(t, err)

		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})

		r.AddConsumerHandler("handler", "topic", subscriber, func(msg *message.Message) error {
			close(handlerStarted)
			<-releaseHandler
			return nil
		})

		go func() {
			assert.NoError(t, r.Run(context.Background()))
		}()
		<-r.Running()

		type closeResult struct {
			Err          error
			MessageAcked bool
		}
		closed := make(chan closeResult, 1)
		go func() {
			err := r.Close()
			closed <- closeResult{Err: err, MessageAcked: isAcked(lateMessage)}
		}()

		waitFor(t, handlerStarted, "handler to start processing the message received while closing")
		// the handler's message loop has ended, so the router has received all messages
		waitForSubscriberStopped(t, logger, "handler", "topic")
		close(releaseHandler)

		select {
		case result := <-closed:
			require.NoError(t, result.Err)
			require.True(t, result.MessageAcked, "Close returned before the message received while closing was processed")
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for router to close")
		}
	}
}

// Like subscribers that deliver messages they have already fetched while the subscription is being closed.
type deliverOnCloseSubscriberMock struct {
	output      chan *message.Message
	lastMessage *message.Message
	closeOnce   sync.Once
}

func newDeliverOnCloseSubscriberMock(lastMessage *message.Message) *deliverOnCloseSubscriberMock {
	return &deliverOnCloseSubscriberMock{
		output:      make(chan *message.Message),
		lastMessage: lastMessage,
	}
}

func (s *deliverOnCloseSubscriberMock) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return s.output, nil
}

func (s *deliverOnCloseSubscriberMock) Close() error {
	s.closeOnce.Do(func() {
		// let the goroutines started by Router.Close run first, so the message arrives while Close is waiting
		runtime.Gosched()
		s.output <- s.lastMessage
		close(s.output)
	})
	return nil
}

func isAcked(msg *message.Message) bool {
	select {
	case <-msg.Acked():
		return true
	default:
		return false
	}
}

func TestRouter_handler_stopped_right_after_it_started(t *testing.T) {
	t.Parallel()

	r, err := message.NewRouter(message.RouterConfig{CloseTimeout: time.Second}, watermill.NopLogger{})
	require.NoError(t, err)

	handler := r.AddConsumerHandler("handler", "topic", newContextSubscriberMock(), func(msg *message.Message) error {
		return nil
	})

	// to keep at least one running handler to prevent router from closing
	r.AddConsumerHandler("other_handler", "topic", newContextSubscriberMock(), func(msg *message.Message) error {
		return nil
	})

	// Taken before the handler is started: Stopped() used to return nil then, and waiting on it blocked forever.
	stopped := handler.Stopped()

	go func() {
		assert.NoError(t, r.Run(context.Background()))
	}()
	defer func() {
		<-r.Running()
		assert.NoError(t, r.Close())
	}()

	// not waiting for r.Running(), as it's closed only after all handlers have started
	waitFor(t, handler.Started(), "handler to start")
	handler.Stop()

	waitFor(t, stopped, "handler to stop")
}
