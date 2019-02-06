package cqrs

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

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
	if err := isPointer(event); err != nil {
		return errors.Wrap(err, "event must be a not nil pointer")
	}

	msg, err := c.marshaler.Marshal(event)
	if err != nil {
		return err
	}

	return c.publisher.Publish(c.topic, msg)
}
