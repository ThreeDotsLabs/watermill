package middleware

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestInstantAck(t *testing.T) {
	producedMessages := message.Messages{message.NewMessage("2", nil)}
	producedErr := errors.New("foo")

	h := InstantAck(func(msg *message.Message) (messages []*message.Message, e error) {
		return producedMessages, producedErr
	})

	msg := message.NewMessage("1", nil)

	handlerMessages, handlerErr := h(msg)
	assert.EqualValues(t, producedMessages, handlerMessages)
	assert.Equal(t, producedErr, handlerErr)

	select {
	case <-msg.Acked():
	// ok
	case <-msg.Nacked():
		t.Fatal("expected ack, not nack")
	default:
		t.Fatal("no ack received")
	}
}
