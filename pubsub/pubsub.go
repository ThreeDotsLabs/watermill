package pubsub

import (
	"time"
	"context"
)

// todo ack support
// todo - should be topic passed to constructor???

type MessagePayload interface{}

// todo - interface??
type Message struct {
	time         time.Time
	acknowledged bool
	payload      MessagePayload

	context context.Context // todo!
}

func NewMessage(payload MessagePayload) Message {
	return Message{time.Now(), true, payload, context.Background()}
}

func (m *Message) Context() context.Context {
	return m.context
}

func (m *Message) WithContext(context context.Context) {
	m.context = context
}

func (m Message) Payload() MessagePayload {
	return m.payload
}

func (m Message) Acknowledged() bool {
	return m.acknowledged
}

func (m Message) Acknowledge() error {
	// todo - implement
	m.acknowledged = true
	return nil
}

func (m Message) Time() time.Time {
	return m.time
}

type PubSub interface {
	Publish(topic string, msgs ...MessagePayload) error
	Subscribe(topic string) (chan Message, error)
	Close() error // todo - needed?
}
