package cqrs

import (
	"context"
	"errors"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type EventBusConfig struct {
	// GeneratePublishTopic is used to generate topic name for publishing event.
	GeneratePublishTopic GenerateEventPublishTopicFn

	// OnPublish is called before sending the event.
	// The *message.Message can be modified.
	//
	// This option is not required.
	OnPublish OnEventSendFn

	// Marshaler is used to marshal and unmarshal events.
	// It is required.
	Marshaler CommandEventMarshaler

	// Logger instance used to log.
	// If not provided, watermill.NopLogger is used.
	Logger watermill.LoggerAdapter
}

func (c *EventBusConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c EventBusConfig) Validate() error {
	var err error

	if c.Marshaler == nil {
		err = errors.Join(err, errors.New("missing Marshaler"))
	}

	if c.GeneratePublishTopic == nil {
		err = errors.Join(err, errors.New("missing GenerateHandlerTopic"))
	}

	return err
}

type GenerateEventPublishTopicFn func(GenerateEventPublishTopicParams) (string, error)

type GenerateEventPublishTopicParams struct {
	EventName string
	Event     any
}

type OnEventSendFn func(params OnEventSendParams) error

type OnEventSendParams struct {
	EventName string
	Event     any

	// Message is never nil and can be modified.
	Message *message.Message
}

// EventBus transports events to event handlers.
type EventBus struct {
	publisher message.Publisher
	config    EventBusConfig
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
		config: EventBusConfig{
			GeneratePublishTopic: func(params GenerateEventPublishTopicParams) (string, error) {
				return generateTopic(params.EventName), nil
			},
			Marshaler: marshaler,
		},
	}, nil
}

// NewEventBusWithConfig creates a new EventBus.
func NewEventBusWithConfig(publisher message.Publisher, config EventBusConfig) (*EventBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}

	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
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
		return fmt.Errorf("cannot generate topic: %w", err)
	}

	msg.SetContext(ctx)

	if c.config.OnPublish != nil {
		err := c.config.OnPublish(OnEventSendParams{
			EventName: eventName,
			Event:     event,
			Message:   msg,
		})
		if err != nil {
			return fmt.Errorf("cannot execute OnPublish: %w", err)
		}
	}

	return c.publisher.Publish(topicName, msg)
}
