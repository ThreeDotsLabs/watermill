package cqrs

import (
	stdErr "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
)

// GenerateEventsTopicFn generates topic for individual event handler.
type GenerateEventsTopicFn func(GenerateEventsTopicParams) string

type GenerateEventsTopicParams struct {
	EventName string
	Handler   EventHandler
}

// GenerateEventsGroupTopicFn generates topic for event handler group.
type GenerateEventsGroupTopicFn func(GenerateEventsGroupTopicParams) string

type GenerateEventsGroupTopicParams struct {
	GroupName string
	Handlers  []GroupEventHandler
}

// todo: rename to EventConfig?
type EventConfig struct {
	// todo: rename
	GenerateIndividualSubscriberTopic GenerateEventsTopicFn
	// todo: rename
	GenerateHandlerGroupTopic GenerateEventsGroupTopicFn

	// todo: rename to nack?
	ErrorOnUnknownEvent bool

	SubscriberConstructor EventsSubscriberConstructor

	Marshaler CommandEventMarshaler
	Logger    watermill.LoggerAdapter
}

func (c *EventConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c EventConfig) Validate() error {
	var err error

	if c.GenerateIndividualSubscriberTopic == nil && c.GenerateHandlerGroupTopic == nil {
		err = stdErr.Join(err, errors.New("GenerateIndividualSubscriberTopic or GenerateHandlerGroupTopic must be set"))
	}

	if c.Marshaler == nil {
		err = stdErr.Join(err, errors.New("missing Marshaler"))
	}

	if c.SubscriberConstructor == nil {
		err = stdErr.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}
