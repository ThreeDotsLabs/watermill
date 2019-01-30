package cqrs

import "github.com/ThreeDotsLabs/watermill/message"

type EventBus struct {
	publisher message.Publisher
	topic     string
	marshaler Marshaler
}

func NewEventBus(
	publisher message.Publisher,
	topic string,
	marshaler Marshaler,
) EventBus {
	return EventBus{publisher, topic, marshaler}
}

func (c EventBus) Publish(event interface{}) error {
	msg, err := c.marshaler.Marshal(event)
	if err != nil {
		panic(err)
	}

	return c.publisher.Publish(c.topic, msg)
}
