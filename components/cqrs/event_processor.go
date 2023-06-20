package cqrs

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// EventProcessor determines which EventHandler should handle event received from event bus.
type EventProcessor struct {
	individualHandlers []EventHandler
	groupEventHandlers map[string][]GroupEventHandler

	config EventConfig
}

// NewEventProcessorWithConfig creates a new EventProcessor.
func NewEventProcessorWithConfig(config EventConfig) (*EventProcessor, error) {
	config.setDefaults()

	if err := config.ValidateForProcessor(); err != nil {
		return nil, errors.Wrap(err, "invalid config EventProcessor")
	}

	return &EventProcessor{
		groupEventHandlers: map[string][]GroupEventHandler{},
		config:             config,
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

	eventProcessorConfig := EventConfig{
		AckOnUnknownEvent: true, // this is the previous default behaviour - keeping backwards compatibility
		GenerateHandlerSubscribeTopic: func(params GenerateEventHandlerSubscribeTopicParams) (string, error) {
			return generateTopic(params.EventName), nil
		},
		SubscriberConstructor: func(params EventsSubscriberConstructorParams) (message.Subscriber, error) {
			return subscriberConstructor(params.HandlerName)
		},
		Marshaler: marshaler,
		Logger:    logger,
	}
	eventProcessorConfig.setDefaults()

	ep, err := NewEventProcessorWithConfig(eventProcessorConfig)
	if err != nil {
		return nil, err
	}

	for _, handler := range individualHandlers {
		ep.AddHandler(handler)
	}

	return ep, nil
}

// AddHandler adds a new EventHandler to the EventProcessor.
//
// It's required to call AddHandlersToRouter to add the handlers to the router after calling AddHandler.
func (p *EventProcessor) AddHandler(handler ...EventHandler) {
	p.individualHandlers = append(p.individualHandlers, handler...)
}

// AddHandlersGroup adds a new list of GroupEventHandler to the EventProcessor.
//
// Compared to AddHandler, AddHandlersGroup allows to have multiple handlers that share the same subscriber instance.
//
// It's required to call AddHandlersToRouter to add the handlers to the router after calling AddHandlersGroup.
// Handlers group needs to be unique within the EventProcessor instance.
func (p *EventProcessor) AddHandlersGroup(handlerName string, handlers ...GroupEventHandler) error {
	if len(handlers) == 0 {
		return errors.New("no handlers provided")
	}
	if _, ok := p.groupEventHandlers[handlerName]; ok {
		return fmt.Errorf("event handler group '%s' already exists", handlerName)
	}

	p.groupEventHandlers[handlerName] = handlers

	return nil
}

// AddHandlersToRouter adds the EventProcessor's handlers to the given router.
// It should be called only once per EventProcessor instance.
func (p EventProcessor) AddHandlersToRouter(r *message.Router) error {
	handlers := p.Handlers()
	if len(handlers) == 0 {
		return errors.New("EventProcessor has no handlers, did you call AddHandler or AddHandlersGroup?")
	}

	for i := range p.individualHandlers {
		handler := p.individualHandlers[i]

		if err := p.validateEvent(handler.NewEvent()); err != nil {
			return errors.Wrapf(err, "invalid event for handler %s", handler.HandlerName())
		}

		handlerName := handler.HandlerName()
		eventName := p.config.Marshaler.Name(handler.NewEvent())

		if p.config.GenerateHandlerSubscribeTopic == nil {
			return errors.New("missing GenerateHandlerSubscribeTopic config option")
		}

		topicName, err := p.config.GenerateHandlerSubscribeTopic(GenerateEventHandlerSubscribeTopicParams{
			EventName:    eventName,
			EventHandler: handler,
		})
		if err != nil {
			return errors.Wrapf(err, "cannot generate topic name for handler %s", handlerName)
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

		subscriber, err := p.config.SubscriberConstructor(EventsSubscriberConstructorParams{
			HandlerName:  handlerName,
			EventHandler: handler,
		})
		if err != nil {
			return errors.Wrap(err, "cannot create subscriber for event processor")
		}

		if err := p.addHandlerToRouter(r, handlerName, topicName, handlerFunc, subscriber); err != nil {
			return err
		}
	}

	for groupName := range p.groupEventHandlers {
		handlersGroup := p.groupEventHandlers[groupName]

		for i, handler := range handlersGroup {
			if err := p.validateEvent(handler.NewEvent()); err != nil {
				return fmt.Errorf(
					"invalid event for handler %T (num %d) in group %s: %w",
					handler,
					i,
					groupName,
					err,
				)
			}
		}

		if p.config.GenerateHandlerGroupSubscribeTopic == nil {
			return errors.New("missing GenerateHandlerGroupSubscribeTopic config option")
		}

		topicName, err := p.config.GenerateHandlerGroupSubscribeTopic(GenerateEventHandlerGroupTopicParams{
			EventGroupName:     groupName,
			EventGroupHandlers: handlersGroup,
		})
		if err != nil {
			return errors.Wrapf(err, "cannot generate topic name for handler group %s", groupName)
		}

		logger := p.config.Logger.With(watermill.LogFields{
			"event_handler_group_name": groupName,
			"topic":                    topicName,
		})

		handlerFunc, err := p.routerHandlerGroupFunc(handlersGroup, groupName, logger)
		if err != nil {
			return err
		}

		subscriber, err := p.config.GroupSubscriberConstructor(EventsGroupSubscriberConstructorParams{
			EventGroupName:     groupName,
			EventGroupHandlers: handlersGroup,
		})
		if err != nil {
			return errors.Wrap(err, "cannot create subscriber for event processor")
		}

		if err := p.addHandlerToRouter(r, groupName, topicName, handlerFunc, subscriber); err != nil {
			return err
		}
	}

	return nil
}

func (p EventProcessor) Handlers() []EventHandler {
	var groupHandlers []EventHandler

	for groupName, handlers := range p.groupEventHandlers {
		for i := range handlers {
			groupHandlers = append(groupHandlers, groupEventHandlerToEventHandlerAdapter{
				GroupEventHandler: p.groupEventHandlers[groupName][i],
				handlerName:       groupName,
			})
		}
	}

	return append(p.individualHandlers, groupHandlers...)
}

func (p EventProcessor) addHandlerToRouter(r *message.Router, handlerName string, topicName string, handlerFunc message.NoPublishHandlerFunc, subscriber message.Subscriber) error {
	logger := p.config.Logger.With(watermill.LogFields{
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

	if err := p.validateEvent(initEvent); err != nil {
		return nil, err
	}

	return func(msg *message.Message) error {
		event := handler.NewEvent()
		messageEventName := p.config.Marshaler.NameFromMessage(msg)

		if messageEventName != expectedEventName {
			// todo: test
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

		if err := p.config.Marshaler.Unmarshal(msg, event); err != nil {
			return err
		}

		handle := func(params OnEventHandleParams) error {
			return params.Handler.Handle(params.Message.Context(), params.Event)
		}
		if p.config.OnHandle != nil {
			handle = p.config.OnHandle
		}

		err := handle(OnEventHandleParams{
			Handler: handler,
			Event:   event,
			Message: msg,
		})
		if err != nil {
			logger.Debug("Error when handling event", watermill.LogFields{"err": err})
			return err
		}

		return nil
	}, nil
}

func (p EventProcessor) routerHandlerGroupFunc(handlers []GroupEventHandler, groupName string, logger watermill.LoggerAdapter) (message.NoPublishHandlerFunc, error) {
	return func(msg *message.Message) error {
		messageEventName := p.config.Marshaler.NameFromMessage(msg)

		for _, handler := range handlers {
			initEvent := handler.NewEvent()
			expectedEventName := p.config.Marshaler.Name(initEvent)

			event := handler.NewEvent()

			if messageEventName != expectedEventName {
				logger.Trace("Received different event type than expected, ignoring", watermill.LogFields{
					"message_uuid":        msg.UUID,
					"expected_event_type": expectedEventName,
					"received_event_type": messageEventName,
				})
				continue
			}

			logger.Debug("Handling event", watermill.LogFields{
				"message_uuid":        msg.UUID,
				"received_event_type": messageEventName,
			})

			if err := p.config.Marshaler.Unmarshal(msg, event); err != nil {
				return err
			}

			handle := func(params OnGroupEventHandleParams) error {
				return params.Handler.Handle(params.Message.Context(), params.Event)
			}
			if p.config.OnGroupHandle != nil {
				handle = p.config.OnGroupHandle
			}

			err := handle(OnGroupEventHandleParams{
				GroupName: groupName,
				Handler:   handler,
				Event:     event,
				Message:   msg,
			})
			if err != nil {
				logger.Debug("Error when handling event", watermill.LogFields{"err": err})
				return err
			}

			return nil
		}

		// todo: test
		if !p.config.AckOnUnknownEvent {
			return fmt.Errorf("no handler found for event %s", p.config.Marshaler.NameFromMessage(msg))
		} else {
			logger.Trace("Received event can't be handled by any handler in handler group", watermill.LogFields{
				"message_uuid":        msg.UUID,
				"received_event_type": messageEventName,
			})
			return nil
		}
	}, nil
}

func (p EventProcessor) validateEvent(event interface{}) error {
	// EventHandler's NewEvent must return a pointer, because it is used to unmarshal
	if err := isPointer(event); err != nil {
		return errors.Wrap(err, "command must be a non-nil pointer")
	}

	return nil
}

type groupEventHandlerToEventHandlerAdapter struct {
	GroupEventHandler
	handlerName string
}

func (g groupEventHandlerToEventHandlerAdapter) HandlerName() string {
	return g.handlerName
}
