package cqrs

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

type EventBus struct {
	publisher message.Publisher
	topic     string
	marshaler CommandEventMarshaler
}

func NewEventBus(
	publisher message.Publisher,
	topic string,
	marshaler CommandEventMarshaler,
) *EventBus {
	if publisher == nil {
		panic("missing publisher")
	}
	if topic == "" {
		panic("missing topic")
	}
	if marshaler == nil {
		panic("missing marshaler")
	}

	return &EventBus{publisher, topic, marshaler}
}

func (c EventBus) Publish(event interface{}) error {
	msg, err := c.marshaler.Marshal(event)
	if err != nil {
		return err
	}

	return c.publisher.Publish(c.topic, msg)
}
