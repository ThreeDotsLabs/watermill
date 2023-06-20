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

type EventsSubscriberConstructorParams struct {
	HandlerName string
	Handler     EventHandler
}

type EventsGroupSubscriberConstructorWithParams func(EventsGroupSubscriberConstructorParams) (message.Subscriber, error)

type EventsGroupSubscriberConstructorParams struct {
	GroupName string
	Handlers  []GroupEventHandler
}

type EventConfig struct {
	// todo: optional when passing to processor?
	// todo: make GenerateHandlerTopic and GenerateHandlerGroupSubscribeTopic optional if it's present?
	GeneratePublishTopic GenerateEventPublishTopicFn

	GenerateHandlerSubscribeTopic GenerateEventHandlerSubscribeTopicFn
	SubscriberConstructor         EventsSubscriberConstructorWithParams

	// todo: validate
	// todo: optional when passing to bus?
	GenerateHandlerGroupSubscribeTopic GenerateEventHandlerGroupSubscribeTopicFn
	// todo: fix validation
	// todo: unify naming and order of fields?
	GroupSubscriberConstructor EventsGroupSubscriberConstructorWithParams

	// OnPublish is called before sending the event.
	// The *message.Message can be modified.
	//
	// This option is not required.
	OnPublish OnEventSendFn

	// OnHandle is called before handling event.
	// OnHandle works in a similar way to middlewares: you can inject additional logic before and after handling a event.
	//
	// Because of that, you need to explicitly call params.Handler.Handle() to handle the event.
	//   func(params OnEventHandleParams) (err error) {
	//       // logic before handle
	//		 //  (...)
	//
	//	     err := params.Handler.Handle(params.Message.Context(), params.Event)
	//
	//       // logic after handle
	//		 //  (...)
	//
	//		 return err
	//	 }
	//
	// OnHandle is not called for handlers group.
	//
	// This option is not required.
	OnHandle OnEventHandleFn

	// OnGroupHandle works like OnHandle, but is called for group handlers instead.
	// OnHandle is not called for handlers group.
	// This option is not required.
	OnGroupHandle OnGroupEventHandleFn

	// AckOnUnknownEvent is used to decide if message should be acked if event has no handler defined.
	AckOnUnknownEvent bool

	// Marshaler is used to marshal and unmarshal events.
	// It is required.
	Marshaler CommandEventMarshaler

	// Logger instance used to log.
	// If not provided, watermill.NopLogger is used.
	Logger watermill.LoggerAdapter
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
