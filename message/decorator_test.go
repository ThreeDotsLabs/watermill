package message_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var noop = func(*message.Message) {}
var closingErr = errors.New("mock error on close")

type mockSubscriber struct {
	ch chan *message.Message
}

func (m mockSubscriber) Subscribe(context.Context, string) (<-chan *message.Message, error) {
	return m.ch, nil
}

func (m mockSubscriber) Close() error {
	close(m.ch)
	return nil
}

func TestMessageTransformSubscriberDecorator_transparent(t *testing.T) {
	sub := mockSubscriber{make(chan *message.Message)}
	decorated, err := message.MessageTransformSubscriberDecorator(noop)(sub)
	require.NoError(t, err)

	messages, err := decorated.Subscribe(context.Background(), "topic")
	require.NoError(t, err)

	richMessage := message.NewMessage("uuid", []byte("serious payloads"))
	richMessage.Metadata.Set("k1", "v1")
	richMessage.Metadata.Set("k2", "v2")

	go func() {
		sub.ch <- richMessage
	}()

	received, all := subscriber.BulkRead(messages, 1, time.Second)
	require.True(t, all)

	assert.True(t, received[0].Equals(richMessage), "expected the message to pass unchanged through decorator")
}

type closingSubscriber struct {
	closed bool
}

func (closingSubscriber) Subscribe(context.Context, string) (<-chan *message.Message, error) {
	return nil, nil
}

func (c *closingSubscriber) Close() error {
	c.closed = true
	return closingErr
}

func TestMessageTransformSubscriberDecorator_Close(t *testing.T) {
	cs := &closingSubscriber{}

	decoratedSub, err := message.MessageTransformSubscriberDecorator(noop)(cs)
	require.NoError(t, err)

	// given
	require.False(t, cs.closed)

	// when
	decoratedCloseErr := decoratedSub.Close()

	// then
	assert.True(
		t,
		cs.closed,
		"expected the Close() call to propagate to decorated subscriber",
	)
	assert.Equal(
		t,
		closingErr,
		decoratedCloseErr,
		"expected the decorator to propagate the closing error from underlying subscriber",
	)
}

func TestMessageTransformSubscriberDecorator_Subscribe(t *testing.T) {
	numMessages := 1000
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NewStdLogger(true, true))

	onMessage := func(msg *message.Message) {
		msg.Metadata.Set("key", "value")
	}
	decorator := message.MessageTransformSubscriberDecorator(onMessage)

	decoratedSub, err := decorator(pubSub)
	require.NoError(t, err)

	messages, err := decoratedSub.Subscribe(context.Background(), "topic")
	require.NoError(t, err)

	sent := message.Messages{}

	go func() {
		for i := 0; i < numMessages; i++ {
			msg := message.NewMessage(strconv.Itoa(i), []byte{})
			sent = append(sent, msg)

			err = pubSub.Publish("topic", msg)
			require.NoError(t, err)
		}
	}()

	received, all := subscriber.BulkRead(messages, numMessages, time.Second)
	require.True(t, all)
	tests.AssertAllMessagesReceived(t, sent, received)

	for _, msg := range received {
		assert.Equal(
			t,
			"value",
			msg.Metadata.Get("key"),
			"expected onMessage callback to have set metadata",
		)
	}
}

type mockPublisher struct {
	published message.Messages
}

func (m *mockPublisher) Publish(topic string, messages ...*message.Message) error {
	m.published = append(m.published, messages...)
	return nil
}

func (m mockPublisher) Close() error {
	return nil
}

func TestMessageTransformPublisherDecorator_transparent(t *testing.T) {
	pub := &mockPublisher{message.Messages{}}
	decorated, err := message.MessageTransformPublisherDecorator(noop)(pub)
	require.NoError(t, err)

	richMessage := message.NewMessage("uuid", []byte("serious payloads"))
	richMessage.Metadata.Set("k1", "v1")
	richMessage.Metadata.Set("k2", "v2")

	require.NoError(t, decorated.Publish("topic", richMessage))

	require.Len(t, pub.published, 1)
	assert.True(t, pub.published[0].Equals(richMessage), "expected the message to pass unchanged through decorator")
}

type closingPublisher struct {
	closed bool
}

func (c *closingPublisher) Publish(topic string, messages ...*message.Message) error {
	return nil
}

func (c *closingPublisher) Close() error {
	c.closed = true
	return closingErr
}

func TestMessageTransformPublisherDecorator_Close(t *testing.T) {
	cp := &closingPublisher{}

	decoratedPub, err := message.MessageTransformPublisherDecorator(noop)(cp)
	require.NoError(t, err)

	// given
	require.False(t, cp.closed)

	// when
	decoratedCloseErr := decoratedPub.Close()

	// then
	assert.True(
		t,
		cp.closed,
		"expected the Close() call to propagate to decorated publisher",
	)
	assert.Equal(
		t,
		closingErr,
		decoratedCloseErr,
		"expected the decorator to propagate the closing error from underlying publisher",
	)
}

func TestMessageTransformPublisherDecorator_Subscribe(t *testing.T) {
	numMessages := 1000
	pub := &mockPublisher{}

	onMessage := func(msg *message.Message) {
		msg.Metadata.Set("key", "value")
	}
	decorator := message.MessageTransformPublisherDecorator(onMessage)
	decoratedPub, err := decorator(pub)
	require.NoError(t, err)

	for i := 0; i < numMessages; i++ {
		msg := message.NewMessage(strconv.Itoa(i), []byte{})
		require.NoError(t, decoratedPub.Publish("topic", msg))
	}

	for i, msg := range pub.published {
		assert.Equal(t, strconv.Itoa(i), msg.UUID, "expected messages to arrive in unchanged order")
		assert.Equal(
			t,
			"value",
			msg.Metadata.Get("key"),
			"expected onMessage callback to have set metadata",
		)
	}
}

func TestMessageTransformer_nil_panics(t *testing.T) {
	require.Panics(
		t,
		func() {
			_ = message.MessageTransformSubscriberDecorator(nil)
		},
		"expected to panic if transform is nil",
	)
	require.Panics(
		t,
		func() {
			_ = message.MessageTransformPublisherDecorator(nil)
		},
		"expected to panic if transform is nil",
	)
}
