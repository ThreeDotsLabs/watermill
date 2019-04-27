package cqrs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

// EventBus transports events to event handlers.
type EventBus struct {
	publisher     message.Publisher
	generateTopic EventTopicGenerator
	marshaler     CommandEventMarshaler
}

func NewEventBus(
	publisher message.Publisher,
	generateTopic EventTopicGenerator,
	marshaler CommandEventMarshaler,
) *EventBus {
	if publisher == nil {
		panic("missing publisher")
	}
	if generateTopic == nil {
		panic("missing generateTopic")
	}
	if marshaler == nil {
		panic("missing marshaler")
	}

	return &EventBus{publisher, generateTopic, marshaler}
}

// Publish sends event to the event bus.
func (c EventBus) Publish(ctx context.Context, event interface{}) error {
	msg, err := c.marshaler.Marshal(event)
	if err != nil {
		return err
	}

	eventName := c.marshaler.Name(event)
	topicName := c.generateTopic(eventName)

	msg.SetContext(ctx)

	return c.publisher.Publish(topicName, msg)
}
