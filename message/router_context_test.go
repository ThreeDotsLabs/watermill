package message_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type namedMockPublisher struct{}

func (namedMockPublisher) Publish(topic string, messages ...*message.Message) error { return nil }
func (namedMockPublisher) Close() error                                             { return nil }
func (namedMockPublisher) String() string {
	return "this publisher implements Stringer"
}

type namedMockSubscriber struct{ ch chan *message.Message }

func (s namedMockSubscriber) Subscribe(context.Context, string) (<-chan *message.Message, error) {
	return s.ch, nil
}
func (s *namedMockSubscriber) Close() error { close(s.ch); return nil }
func (namedMockSubscriber) String() string {
	return "this subscriber implements Stringer"
}

func TestRouter_Context_Stringer(t *testing.T) {
	// If a publisher or subscriber implements Stringer, it's name is the result of String().
	// The messages processed by a router handler should have publisher and subscriber name in their context.

	// given
	capturedMessages := make(chan *message.Message)
	router, handlerFunc := setupPubsubNameTests(t, capturedMessages)

	sub := &namedMockSubscriber{make(chan *message.Message)}
	pub := namedMockPublisher{}

	handlerName := "handler_name_stringer_test"
	router.AddHandler(
		handlerName,
		"sub-topic",
		sub,
		"pub-topic",
		pub,
		handlerFunc,
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

	// when
	sub.ch <- message.NewMessage("", []byte{})
	capturedMsg := <-capturedMessages

	ctx := capturedMsg.Context()

	// then
	require.Equal(t, handlerName, message.HandlerNameFromCtx(ctx))
	require.Equal(t, sub.String(), message.SubscriberNameFromCtx(ctx))
	require.Equal(t, pub.String(), message.PublisherNameFromCtx(ctx))
	require.Equal(t, "sub-topic", message.SubscribeTopicFromCtx(ctx))
	require.Equal(t, "pub-topic", message.PublishTopicFromCtx(ctx))
}

type unnamedMockPublisher struct{}

func (unnamedMockPublisher) Publish(topic string, messages ...*message.Message) error { return nil }
func (unnamedMockPublisher) Close() error                                             { return nil }

type unnamedMockSubscriber struct{ ch chan *message.Message }

func (s unnamedMockSubscriber) Subscribe(context.Context, string) (<-chan *message.Message, error) {
	return s.ch, nil
}
func (s *unnamedMockSubscriber) Close() error { close(s.ch); return nil }

func TestRouter_Context_TypeName(t *testing.T) {
	// If a publisher or subscriber does not implement Stringer, it's name is the type name.
	// The messages processed by a router handler should have publisher and subscriber name in their context.

	// given
	capturedMessages := make(chan *message.Message)
	router, handlerFunc := setupPubsubNameTests(t, capturedMessages)

	sub := &unnamedMockSubscriber{make(chan *message.Message)}
	pub := unnamedMockPublisher{}

	handlerName := "handler_name_typename_test"
	router.AddHandler(
		handlerName,
		"",
		sub,
		"",
		pub,
		handlerFunc,
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

	// when
	sub.ch <- message.NewMessage("", []byte{})
	capturedMsg := <-capturedMessages

	ctx := capturedMsg.Context()

	// then
	require.Equal(t, handlerName, message.HandlerNameFromCtx(ctx))
	require.Equal(t, "message_test.unnamedMockSubscriber", message.SubscriberNameFromCtx(ctx))
	require.Equal(t, "message_test.unnamedMockPublisher", message.PublisherNameFromCtx(ctx))
}

func setupPubsubNameTests(t *testing.T, capturedMessages chan (*message.Message)) (*message.Router, message.HandlerFunc) {
	logger := watermill.NewStdLogger(true, true)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		capturedMessages <- msg
		require.True(t, msg.Ack())
		return message.Messages{message.NewMessage(msg.UUID+"_copy", msg.Payload)}, nil
	}

	return router, handlerFunc
}
