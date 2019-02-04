package message_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message/subscriber"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
)

var noop = func(*message.Message) {}

func TestMessageTransformer_Subscribe(t *testing.T) {
	numMessages := 1000
	pubsub := gochannel.NewGoChannel(0, watermill.NewStdLogger(true, true), time.Second)

	onMessage := func(msg *message.Message) {
		msg.Metadata.Set("key", "value")
	}
	decorator := message.MessageTransformSubscriberDecorator(onMessage)

	decoratedSub, err := decorator(pubsub.(message.Subscriber))
	require.NoError(t, err)

	messages, err := decoratedSub.Subscribe("topic")
	require.NoError(t, err)

	go func() {
		for i := 0; i < numMessages; i++ {
			err = pubsub.Publish("topic", message.NewMessage(strconv.Itoa(i), []byte{}))
			require.NoError(t, err)
		}
	}()

	received, all := subscriber.BulkRead(messages, numMessages, time.Second)
	require.True(t, all)

	for _, msg := range received {
		assert.Equal(
			t,
			"value",
			msg.Metadata.Get("key"),
			"expected onMessage callback to have set metadata",
		)
	}
}

type mockSubscriber struct {
	ch chan *message.Message
}

func (m mockSubscriber) Subscribe(topic string) (chan *message.Message, error) { return m.ch, nil }
func (m mockSubscriber) Close() error                                          { close(m.ch); return nil }

func TestMessageTransformer_transparent(t *testing.T) {
	sub := mockSubscriber{make(chan *message.Message)}
	decorated, err := message.MessageTransformSubscriberDecorator(noop)(sub)
	require.NoError(t, err)

	messages, err := decorated.Subscribe("topic")
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

func TestMessageTransformer_nil_panics(t *testing.T) {
	require.Panics(t, func() {
		_ = message.MessageTransformSubscriberDecorator(nil)
	})
}

var closingErr = errors.New("mock error on close")

type closingSubscriber struct {
	closed bool
}

func (closingSubscriber) Subscribe(topic string) (chan *message.Message, error) { return nil, nil }
func (c *closingSubscriber) Close() error {
	c.closed = true
	return closingErr
}

func TestMessageTransformer_Close(t *testing.T) {
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
