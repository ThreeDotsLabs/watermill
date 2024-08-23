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

	go func() {
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

	handler := r.AddNoPublisherHandler(
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

	handler := r.AddNoPublisherHandler(
		"test_subscriber_1",
		subscribeTopic1,
		sub,
		func(msg *message.Message) error {
			receivedMessagesCh1 <- msg
			return nil
		},
	)

	// to keep at least one running handler to prevent router from closing
	r.AddNoPublisherHandler(
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
	r.AddNoPublisherHandler(
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

	r.AddNoPublisherHandler(
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

	r.AddNoPublisherHandler(
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

	r.AddNoPublisherHandler(
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

	r.AddNoPublisherHandler(
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
	}, 1*time.Second, 1*time.Millisecond, "Router should be closed after all handlers are stopped")

	assert.Empty(t, logger.Captured()[watermill.ErrorLogLevel], "No error should be logged when context is canceled")
}

func TestRouter_stopping_all_handlers_logs_error(t *testing.T) {
	t.Parallel()

	pub, sub := createPubSub()
	defer func() {
		assert.NoError(t, pub.Close())
		assert.NoError(t, sub.Close())
	}()

	logger := watermill.NewCaptureLogger()

	r, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	r.AddNoPublisherHandler(
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

	// Stop the subscriber - this should close the router with an error
	err = sub.Close()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return r.IsClosed()
	}, 1*time.Second, 1*time.Millisecond, "Router should be closed after all handlers are stopped")

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
