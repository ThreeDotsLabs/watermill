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
	// todo: make GenerateHandlerTopic and GenerateHandlerGroupTopic optional if it's present?
	GenerateTopic GenerateEventTopicFn

	// todo: validate
	// todo: optional when passing to bus?
	GenerateHandlerGroupTopic GenerateEventGroupHandlerTopicFn

	OnSend        OnEventSendFn
	OnHandle      OnEventHandleFn
	OnGroupHandle OnGroupEventHandleFn

	AckOnUnknownEvent bool

	SubscriberConstructor EventsSubscriberConstructorWithParams

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

	// todo: different validation for bus and non-bus
	if c.GenerateTopic == nil && c.GenerateHandlerGroupTopic == nil {
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

type GenerateEventTopicFn func(GenerateEventTopicParams) (string, error)

type GenerateEventTopicParams interface {
	EventName() string
}

type GenerateEventBusTopicParams interface {
	GenerateEventTopicParams
	Event() any
}

type generateEventTopicParams struct {
	eventName string
	event     any
}

func (g generateEventTopicParams) EventName() string {
	return g.eventName
}

func (g generateEventTopicParams) Event() any {
	return g.event
}

type GenerateEventHandlerTopicParams interface {
	GenerateEventTopicParams
	EventHandler() EventHandler
}

type generateEventHandlerTopicParams struct {
	eventName    string
	eventHandler EventHandler
}

func (g generateEventHandlerTopicParams) EventName() string {
	return g.eventName
}

func (g generateEventHandlerTopicParams) EventHandler() EventHandler {
	return g.eventHandler
}

type GenerateEventGroupHandlerTopicFn func(GenerateEventHandlerGroupTopicParams) (string, error)

type GenerateEventHandlerGroupTopicParams interface {
	EventGroupName() string
	EventGroupHandlers() []GroupEventHandler
}

type generateEventHandlerGroupTopicParams struct {
	eventGroupName     string
	eventGroupHandlers []GroupEventHandler
}

func (g generateEventHandlerGroupTopicParams) EventGroupName() string {
	return g.eventGroupName
}

func (g generateEventHandlerGroupTopicParams) EventGroupHandlers() []GroupEventHandler {
	return g.eventGroupHandlers
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
