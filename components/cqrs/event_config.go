package cqrs

import (
	stdErrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// todo: rename to EventConfig?
type EventConfig struct {
	// todo: optional when passing to processor?
	// todo: make GenerateHandlerTopic and GenerateHandlerGroupTopic optional if it's present?
	GenerateBusTopic GenerateEventBusTopicFn

	// todo: validate
	// todo: optional when passing to bus?
	GenerateHandlerTopic      GenerateEventHandlerTopicFn
	GenerateHandlerGroupTopic GenerateEventHandlerGroupTopicFn

	OnSend        OnEventSendFn
	OnHandle      OnEventHandleFn
	OnGroupHandle OnGroupEventHandleFn

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

	if c.GenerateHandlerTopic == nil && c.GenerateHandlerGroupTopic == nil {
		err = stdErrors.Join(err, errors.New("GenerateHandlerTopic or GenerateHandlerGroupTopic is required"))
	}

	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}

type GenerateEventBusTopicFn func(GenerateEventBusTopicParams) (string, error)

type GenerateEventBusTopicParams struct {
	EventName string
	Event     any
}

type GenerateEventHandlerTopicFn func(GenerateEventHandlerTopicParams) (string, error)

type GenerateEventHandlerTopicParams struct {
	EventName string
	Handler   EventHandler
}

type GenerateEventHandlerGroupTopicFn func(GenerateEventHandlerGroupTopicParams) (string, error)

type GenerateEventHandlerGroupTopicParams struct {
	GroupName     string
	GroupHandlers []GroupEventHandler
}

type OnEventSendFn func(params OnEventSendParams) error

type OnEventSendParams struct {
	EventName string
	Event     any
	Message   *message.Message
}

type OnEventHandleFn func(params OnEventHandleParams) error

type OnEventHandleParams struct {
	Handler EventHandler
	Event   any
	// todo: doc that always present
	Message *message.Message
}

type OnGroupEventHandleFn func(params OnGroupEventHandleParams) error

type OnGroupEventHandleParams struct {
	GroupName string
	Handler   GroupEventHandler
	Event     any
	// todo: doc that always present
	Message *message.Message
}
