package cqrs

import (
	stdErrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// EventsSubscriberConstructor creates a subscriber for EventHandler.
// It allows you to create separated customized Subscriber for every command handler.
//
// When handler groups are used, handler group is passed as handlerName.
// Deprecated: please use EventsSubscriberConstructorWithParams instead.
type EventsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

type EventsSubscriberConstructorWithParams func(EventsSubscriberConstructorParams) (message.Subscriber, error)

type EventsSubscriberConstructorParams interface {
	HandlerName() string
}

type eventsSubscriberConstructorParams struct {
	handlerName string
	handler     EventHandler
}

func (e eventsSubscriberConstructorParams) HandlerName() string {
	return e.handlerName
}

func (e eventsSubscriberConstructorParams) Handler() EventHandler {
	return e.handler
}

type EventsGroupSubscriberConstructorParams interface {
	EventsSubscriberConstructorParams

	GroupName() string
	Handlers() []GroupEventHandler
}

type eventsGroupSubscriberConstructorParams struct {
	handlerName string
	groupName   string
	handlers    []GroupEventHandler
}

func (e eventsGroupSubscriberConstructorParams) HandlerName() string {
	return e.handlerName
}

func (e eventsGroupSubscriberConstructorParams) GroupName() string {
	return e.groupName
}

func (e eventsGroupSubscriberConstructorParams) Handlers() []GroupEventHandler {
	return e.handlers
}

type EventConfig struct {
	// todo: optional when passing to processor?
	// todo: make GenerateHandlerTopic and GenerateHandlerGroupSubscribeTopic optional if it's present?
	GeneratePublishTopic GenerateEventPublishTopicFn

	GenerateHandlerSubscribeTopic GenerateEventHandlerSubscribeTopicFn

	// todo: validate
	// todo: optional when passing to bus?
	GenerateHandlerGroupSubscribeTopic GenerateEventHandlerGroupSubscribeTopicFn

	SubscriberConstructor EventsSubscriberConstructorWithParams

	OnPublish     OnEventSendFn
	OnHandle      OnEventHandleFn
	OnGroupHandle OnGroupEventHandleFn

	AckOnUnknownEvent bool

	Marshaler CommandEventMarshaler
	Logger    watermill.LoggerAdapter
}

func (c *EventConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c EventConfig) validateCommon() error {
	var err error

	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	return err
}

func (c EventConfig) ValidateForProcessor() error {
	var err error

	err = stdErrors.Join(err, c.validateCommon())

	if c.GenerateHandlerSubscribeTopic == nil && c.GenerateHandlerGroupSubscribeTopic == nil {
		err = stdErrors.Join(err, errors.New(
			"GenerateHandlerSubscribeTopic and GenerateHandlerGroupSubscribeTopic are missing, one of them is required",
		))
	}

	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}

func (c EventConfig) ValidateForBus() error {
	var err error

	err = stdErrors.Join(err, c.validateCommon())

	if c.GeneratePublishTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateHandlerTopic"))
	}

	return err
}

type GenerateEventPublishTopicFn func(GenerateEventPublishTopicParams) (string, error)

type GenerateEventPublishTopicParams struct {
	EventName string
	Event     any
}

type GenerateEventHandlerSubscribeTopicFn func(GenerateEventHandlerSubscribeTopicParams) (string, error)

type GenerateEventHandlerSubscribeTopicParams struct {
	EventName    string
	EventHandler EventHandler
}

type GenerateEventHandlerGroupSubscribeTopicFn func(GenerateEventHandlerGroupTopicParams) (string, error)

type GenerateEventHandlerGroupTopicParams struct {
	EventGroupName     string
	EventGroupHandlers []GroupEventHandler
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
