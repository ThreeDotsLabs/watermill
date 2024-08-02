package cqrs

import (
	"errors"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type EventProcessorConfig struct {
	// GenerateSubscribeTopic is used to generate topic for subscribing to events.
	// If event processor is using handler groups, GenerateSubscribeTopic is used instead.
	GenerateSubscribeTopic EventProcessorGenerateSubscribeTopicFn

	// SubscriberConstructor is used to create subscriber for EventHandler.
	//
	// This function is called for every EventHandler instance.
	// If you want to re-use one subscriber for multiple handlers, use GroupEventProcessor instead.
	SubscriberConstructor EventProcessorSubscriberConstructorFn

	// OnHandle is called before handling event.
	// OnHandle works in a similar way to middlewares: you can inject additional logic before and after handling a event.
	//
	// Because of that, you need to explicitly call params.Handler.Handle() to handle the event.
	//
	//  func(params EventProcessorOnHandleParams) (err error) {
	//      // logic before handle
	//      //  (...)
	//
	//      err := params.Handler.Handle(params.Message.Context(), params.Event)
	//
	//      // logic after handle
	//      //  (...)
	//
	//      return err
	//  }
	//
	// This option is not required.
	OnHandle EventProcessorOnHandleFn

	// AckOnUnknownEvent is used to decide if message should be acked if event has no handler defined.
	AckOnUnknownEvent bool

	// Marshaler is used to marshal and unmarshal events.
	// It is required.
	Marshaler CommandEventMarshaler

	// Logger instance used to log.
	// If not provided, watermill.NopLogger is used.
	Logger watermill.LoggerAdapter

	// disableRouterAutoAddHandlers is used to keep backwards compatibility.
	// it is set when EventProcessor is created by NewEventProcessor.
	// Deprecated: please migrate to NewEventProcessorWithConfig.
	disableRouterAutoAddHandlers bool
}

func (c *EventProcessorConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c EventProcessorConfig) Validate() error {
	var err error

	if c.Marshaler == nil {
		err = errors.Join(err, errors.New("missing Marshaler"))
	}

	if c.GenerateSubscribeTopic == nil {
		err = errors.Join(err, errors.New("missing GenerateHandlerTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = errors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}

type EventProcessorGenerateSubscribeTopicFn func(EventProcessorGenerateSubscribeTopicParams) (string, error)

type EventProcessorGenerateSubscribeTopicParams struct {
	EventName    string
	EventHandler EventHandler
}

type EventProcessorSubscriberConstructorFn func(EventProcessorSubscriberConstructorParams) (message.Subscriber, error)

type EventProcessorSubscriberConstructorParams struct {
	HandlerName  string
	EventHandler EventHandler
}

type EventProcessorOnHandleFn func(params EventProcessorOnHandleParams) error

type EventProcessorOnHandleParams struct {
	Handler EventHandler

	Event     any
	EventName string

	// Message is never nil and can be modified.
	Message *message.Message
}

// EventProcessor determines which EventHandler should handle event received from event bus.
type EventProcessor struct {
	router   *message.Router
	handlers []EventHandler
	config   EventProcessorConfig
}

// NewEventProcessorWithConfig creates a new EventProcessor.
func NewEventProcessorWithConfig(router *message.Router, config EventProcessorConfig) (*EventProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config EventProcessor: %w", err)
	}
	if router == nil && !config.disableRouterAutoAddHandlers {
		return nil, errors.New("missing router")
	}

	return &EventProcessor{
		router: router,
		config: config,
	}, nil
}

// NewEventProcessor creates a new EventProcessor.
// Deprecated. Use NewEventProcessorWithConfig instead.
func NewEventProcessor(
	individualHandlers []EventHandler,
	generateTopic func(eventName string) string,
	subscriberConstructor EventsSubscriberConstructor,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
) (*EventProcessor, error) {
	if len(individualHandlers) == 0 {
		return nil, errors.New("missing handlers")
	}
	if generateTopic == nil {
		return nil, errors.New("nil generateTopic")
	}
	if subscriberConstructor == nil {
		return nil, errors.New("missing subscriberConstructor")
	}
	if marshaler == nil {
		return nil, errors.New("missing marshaler")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	eventProcessorConfig := EventProcessorConfig{
		AckOnUnknownEvent: true, // this is the previous default behaviour - keeping backwards compatibility
		GenerateSubscribeTopic: func(params EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return generateTopic(params.EventName), nil
		},
		SubscriberConstructor: func(params EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return subscriberConstructor(params.HandlerName)
		},
		Marshaler:                    marshaler,
		Logger:                       logger,
		disableRouterAutoAddHandlers: true,
	}
	eventProcessorConfig.setDefaults()

	ep, err := NewEventProcessorWithConfig(nil, eventProcessorConfig)
	if err != nil {
		return nil, err
	}

	for _, handler := range individualHandlers {
		if err := ep.AddHandlers(handler); err != nil {
			return nil, err
		}
	}

	return ep, nil
}

// EventsSubscriberConstructor creates a subscriber for EventHandler.
// It allows you to create separated customized Subscriber for every command handler.
//
// When handler groups are used, handler group is passed as handlerName.
// Deprecated: please use EventProcessorSubscriberConstructorFn instead.
type EventsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// AddHandlers adds a new EventHandler to the EventProcessor and adds it to the router.
func (p *EventProcessor) AddHandlers(handlers ...EventHandler) error {
	if p.config.disableRouterAutoAddHandlers {
		p.handlers = append(p.handlers, handlers...)
		return nil
	}

	for _, handler := range handlers {
		if err := p.addHandlerToRouter(p.router, handler); err != nil {
			return err
		}

		p.handlers = append(p.handlers, handler)
	}

	return nil
}

// AddHandlersToRouter adds the EventProcessor's handlers to the given router.
// It should be called only once per EventProcessor instance.
//
// It is required to call AddHandlersToRouter only if command processor is created with NewEventProcessor (disableRouterAutoAddHandlers is set to true).
// Deprecated: please migrate to event processor created by NewEventProcessorWithConfig.
func (p EventProcessor) AddHandlersToRouter(r *message.Router) error {
	if !p.config.disableRouterAutoAddHandlers {
		return errors.New("AddHandlersToRouter should be called only when using deprecated NewEventProcessor")
	}

	for i := range p.handlers {
		handler := p.handlers[i]

		if err := p.addHandlerToRouter(r, handler); err != nil {
			return err
		}
	}

	return nil
}

func (p EventProcessor) addHandlerToRouter(r *message.Router, handler EventHandler) error {
	if err := validateEvent(handler.NewEvent()); err != nil {
		return fmt.Errorf("invalid event for handler %s: %w", handler.HandlerName(), err)
	}

	handlerName := handler.HandlerName()
	eventName := p.config.Marshaler.Name(handler.NewEvent())

	topicName, err := p.config.GenerateSubscribeTopic(EventProcessorGenerateSubscribeTopicParams{
		EventName:    eventName,
		EventHandler: handler,
	})
	if err != nil {
		return fmt.Errorf("cannot generate topic name for handler %s: %w", handlerName, err)
	}

	logger := p.config.Logger.With(watermill.LogFields{
		"event_handler_name": handlerName,
		"topic":              topicName,
	})

	handlerFunc, err := p.routerHandlerFunc(handler, logger)
	if err != nil {
		return err
	}

	if p.config.SubscriberConstructor == nil {
		return errors.New("missing SubscriberConstructor config option")
	}

	subscriber, err := p.config.SubscriberConstructor(EventProcessorSubscriberConstructorParams{
		HandlerName:  handlerName,
		EventHandler: handler,
	})
	if err != nil {
		return fmt.Errorf("cannot create subscriber for event processor: %w", err)
	}

	if err := addHandlerToRouter(p.config.Logger, r, handlerName, topicName, handlerFunc, subscriber); err != nil {
		return err
	}

	return nil
}

func (p EventProcessor) Handlers() []EventHandler {
	return p.handlers
}

func addHandlerToRouter(logger watermill.LoggerAdapter, r *message.Router, handlerName string, topicName string, handlerFunc message.NoPublishHandlerFunc, subscriber message.Subscriber) error {
	logger = logger.With(watermill.LogFields{
		"event_handler_name": handlerName,
		"topic":              topicName,
	})

	logger.Debug("Adding CQRS event handler to router", nil)

	r.AddNoPublisherHandler(
		handlerName,
		topicName,
		subscriber,
		handlerFunc,
	)

	return nil
}

func (p EventProcessor) routerHandlerFunc(handler EventHandler, logger watermill.LoggerAdapter) (message.NoPublishHandlerFunc, error) {
	initEvent := handler.NewEvent()
	expectedEventName := p.config.Marshaler.Name(initEvent)

	if err := validateEvent(initEvent); err != nil {
		return nil, err
	}

	return func(msg *message.Message) error {
		event := handler.NewEvent()
		messageEventName := p.config.Marshaler.NameFromMessage(msg)

		if messageEventName != expectedEventName {
			if !p.config.AckOnUnknownEvent {
				return fmt.Errorf("received unexpected event type %s, expected %s", messageEventName, expectedEventName)
			} else {
				logger.Trace("Received different event type than expected, ignoring", watermill.LogFields{
					"message_uuid":        msg.UUID,
					"expected_event_type": expectedEventName,
					"received_event_type": messageEventName,
				})
				return nil
			}
		}

		logger.Debug("Handling event", watermill.LogFields{
			"message_uuid":        msg.UUID,
			"received_event_type": messageEventName,
		})

		ctx := CtxWithOriginalMessage(msg.Context(), msg)
		msg.SetContext(ctx)

		if err := p.config.Marshaler.Unmarshal(msg, event); err != nil {
			return err
		}

		handle := func(params EventProcessorOnHandleParams) error {
			return params.Handler.Handle(ctx, params.Event)
		}
		if p.config.OnHandle != nil {
			handle = p.config.OnHandle
		}

		err := handle(EventProcessorOnHandleParams{
			Handler:   handler,
			Event:     event,
			EventName: messageEventName,
			Message:   msg,
		})
		if err != nil {
			logger.Debug("Error when handling event", watermill.LogFields{"err": err})
			return err
		}

		return nil
	}, nil
}

func validateEvent(event interface{}) error {
	// EventHandler's NewEvent must return a pointer, because it is used to unmarshal
	if err := isPointer(event); err != nil {
		return fmt.Errorf("command must be a non-nil pointer: %w", err)
	}

	return nil
}
