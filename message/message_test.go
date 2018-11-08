package message

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestMessage_Ack(t *testing.T) {
	msg := &message.Message{}
	msg.Ack()

	assertAcked(t, msg)
	assertNoNack(t, msg)
}

func TestMessage_Ack_idempotent(t *testing.T) {
	msg := &message.Message{}
	msg.Ack()
	msg.Ack()

	assertAcked(t, msg)
}

func TestMessage_Ack_already_Nack(t *testing.T) {
	msg := &message.Message{}
	msg.Nack()

	assert.Panics(t, func() {
		msg.Ack()
	})
}

func TestMessage_Nack(t *testing.T) {
	msg := &message.Message{}
	msg.Nack()

	assertNoAck(t, msg)
	assertNacked(t, msg)
}

func TestMessage_Nack_idempotent(t *testing.T) {
	msg := &message.Message{}
	msg.Nack()
	msg.Nack()

	assertNacked(t, msg)
}

func TestMessage_Nack_already_Ack(t *testing.T) {
	msg := &message.Message{}
	msg.Ack()

	assert.Panics(t, func() {
		msg.Nack()
	})
}

func assertAcked(t *testing.T, msg *message.Message) {
	select {
	case <-msg.Acked():
		// ok
	default:
		t.Fatal("no ack received")
	}
}

func assertNacked(t *testing.T, msg *message.Message) {
	select {
	case <-msg.Nacked():
		// ok
	default:
		t.Fatal("no ack received")
	}
}

func assertNoAck(t *testing.T, msg *message.Message) {
	select {
	case <-msg.Acked():
		t.Fatal("nack should be not sent")
	default:
		// ok
	}
}

func assertNoNack(t *testing.T, msg *message.Message) {
	select {
	case <-msg.Nacked():
		t.Fatal("nack should be not sent")
	default:
		// ok
	}
}
