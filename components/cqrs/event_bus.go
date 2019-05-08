package cqrs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// EventBus transports events to event handlers.
type EventBus struct {
	publisher     message.Publisher
	generateTopic func(eventName string) string
	marshaler     CommandEventMarshaler
}

func NewEventBus(
	publisher message.Publisher,
	generateTopic func(eventName string) string,
	marshaler CommandEventMarshaler,
) (*EventBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}
	if generateTopic == nil {
		return nil, errors.New("missing generateTopic")
	}
	if marshaler == nil {
		return nil, errors.New("missing marshaler")
	}

	return &EventBus{publisher, generateTopic, marshaler}, nil
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
