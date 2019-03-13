package cqrs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

// EventBus transports events to event handlers.
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

// Send sends command to the event bus.
func (c EventBus) Publish(ctx context.Context, event interface{}) error {
	msg, err := c.marshaler.Marshal(event)
	if err != nil {
		return err
	}

	msg.SetContext(ctx)

	return c.publisher.Publish(c.topic, msg)
}
