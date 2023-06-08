package cqrs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// EventBus transports events to event handlers.
type EventBus struct {
	publisher message.Publisher
	config    EventConfig
}

// NewEventBus creates a new CommandBus.
// Deprecated: use NewEventBusWithConfig instead.
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

	return &EventBus{
		publisher: publisher,
		config: EventConfig{
			GeneratePublishTopic: func(params GenerateEventPublishTopicParams) (string, error) {
				return generateTopic(params.EventName), nil
			},
			Marshaler: marshaler,
		},
	}, nil
}

// NewEventBusWithConfig creates a new EventBus.
func NewEventBusWithConfig(publisher message.Publisher, config EventConfig) (*EventBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}

	config.setDefaults()
	if err := config.ValidateForBus(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &EventBus{publisher, config}, nil
}

// Publish sends event to the event bus.
func (c EventBus) Publish(ctx context.Context, event any) error {
	msg, err := c.config.Marshaler.Marshal(event)
	if err != nil {
		return err
	}

	eventName := c.config.Marshaler.Name(event)
	topicName, err := c.config.GeneratePublishTopic(GenerateEventPublishTopicParams{
		EventName: eventName,
		Event:     event,
	})
	if err != nil {
		return errors.Wrap(err, "cannot generate topic")
	}

	msg.SetContext(ctx)

	if c.config.OnSend != nil {
		err := c.config.OnSend(OnEventSendParams{
			EventName: eventName,
			Event:     event,
			Message:   msg,
		})
		if err != nil {
			return errors.Wrap(err, "cannot execute OnSend")
		}
	}

	return c.publisher.Publish(topicName, msg)
}
