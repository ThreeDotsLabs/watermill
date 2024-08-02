package cqrs

import (
	"errors"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type EventGroupProcessorConfig struct {
	// GenerateSubscribeTopic is used to generate topic for subscribing to events for handler groups.
	// This option is required for EventProcessor if handler groups are used.
	GenerateSubscribeTopic EventGroupProcessorGenerateSubscribeTopicFn

	// SubscriberConstructor is used to create subscriber for GroupEventHandler.
	// This function is called for every events group once - thanks to that it's possible to have one subscription per group.
	// It's useful, when we are processing events from one stream and we want to do it in order.
	SubscriberConstructor EventGroupProcessorSubscriberConstructorFn

	// OnHandle is called before handling event.
	// OnHandle works in a similar way to middlewares: you can inject additional logic before and after handling a event.
	//
	// Because of that, you need to explicitly call params.Handler.Handle() to handle the event.
	//
	//  func(params EventGroupProcessorOnHandleParams) (err error) {
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
	OnHandle EventGroupProcessorOnHandleFn

	// AckOnUnknownEvent is used to decide if message should be acked if event has no handler defined.
	AckOnUnknownEvent bool

	// Marshaler is used to marshal and unmarshal events.
	// It is required.
	Marshaler CommandEventMarshaler

	// Logger instance used to log.
	// If not provided, watermill.NopLogger is used.
	Logger watermill.LoggerAdapter
}

func (c *EventGroupProcessorConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c EventGroupProcessorConfig) Validate() error {
	var err error

	if c.Marshaler == nil {
		err = errors.Join(err, errors.New("missing Marshaler"))
	}

	if c.GenerateSubscribeTopic == nil {
		err = errors.Join(err, errors.New("missing GenerateHandlerGroupTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = errors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}

type EventGroupProcessorGenerateSubscribeTopicFn func(EventGroupProcessorGenerateSubscribeTopicParams) (string, error)

type EventGroupProcessorGenerateSubscribeTopicParams struct {
	EventGroupName     string
	EventGroupHandlers []GroupEventHandler
}

type EventGroupProcessorSubscriberConstructorFn func(EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error)

type EventGroupProcessorSubscriberConstructorParams struct {
	EventGroupName     string
	EventGroupHandlers []GroupEventHandler
}

type EventGroupProcessorOnHandleFn func(params EventGroupProcessorOnHandleParams) error

type EventGroupProcessorOnHandleParams struct {
	GroupName string
	Handler   GroupEventHandler

	Event     any
	EventName string

	// Message is never nil and can be modified.
	Message *message.Message
}

// EventGroupProcessor determines which EventHandler should handle event received from event bus.
// Compared to EventProcessor, EventGroupProcessor allows to have multiple handlers that share the same subscriber instance.
type EventGroupProcessor struct {
	router *message.Router

	groupEventHandlers map[string][]GroupEventHandler

	config EventGroupProcessorConfig
}

// NewEventGroupProcessorWithConfig creates a new EventGroupProcessor.
func NewEventGroupProcessorWithConfig(router *message.Router, config EventGroupProcessorConfig) (*EventGroupProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config EventProcessor: %w", err)
	}
	if router == nil {
		return nil, errors.New("missing router")
	}

	return &EventGroupProcessor{
		router:             router,
		groupEventHandlers: map[string][]GroupEventHandler{},
		config:             config,
	}, nil
}

// AddHandlersGroup adds a new list of GroupEventHandler to the EventGroupProcessor and adds it to the router.
//
// Compared to AddHandlers, AddHandlersGroup allows to have multiple handlers that share the same subscriber instance.
//
// Handlers group needs to be unique within the EventProcessor instance.
//
// Handler group name is used as handler's name in router.
func (p *EventGroupProcessor) AddHandlersGroup(groupName string, handlers ...GroupEventHandler) error {
	if len(handlers) == 0 {
		return errors.New("no handlers provided")
	}
	if _, ok := p.groupEventHandlers[groupName]; ok {
		return fmt.Errorf("event handler group '%s' already exists", groupName)
	}

	if err := p.addHandlerToRouter(p.router, groupName, handlers); err != nil {
		return err
	}

	p.groupEventHandlers[groupName] = handlers

	return nil
}

func (p EventGroupProcessor) addHandlerToRouter(r *message.Router, groupName string, handlersGroup []GroupEventHandler) error {
	for i, handler := range handlersGroup {
		if err := validateEvent(handler.NewEvent()); err != nil {
			return fmt.Errorf(
				"invalid event for handler %T (num %d) in group %s: %w",
				handler,
				i,
				groupName,
				err,
			)
		}
	}

	topicName, err := p.config.GenerateSubscribeTopic(EventGroupProcessorGenerateSubscribeTopicParams{
		EventGroupName:     groupName,
		EventGroupHandlers: handlersGroup,
	})
	if err != nil {
		return fmt.Errorf("cannot generate topic name for handler group %s: %w", groupName, err)
	}

	logger := p.config.Logger.With(watermill.LogFields{
		"event_handler_group_name": groupName,
		"topic":                    topicName,
	})

	handlerFunc, err := p.routerHandlerGroupFunc(handlersGroup, groupName, logger)
	if err != nil {
		return err
	}

	subscriber, err := p.config.SubscriberConstructor(EventGroupProcessorSubscriberConstructorParams{
		EventGroupName:     groupName,
		EventGroupHandlers: handlersGroup,
	})
	if err != nil {
		return fmt.Errorf("cannot create subscriber for event processor: %w", err)
	}

	if err := addHandlerToRouter(p.config.Logger, r, groupName, topicName, handlerFunc, subscriber); err != nil {
		return err
	}

	return nil
}

func (p EventGroupProcessor) routerHandlerGroupFunc(handlers []GroupEventHandler, groupName string, logger watermill.LoggerAdapter) (message.NoPublishHandlerFunc, error) {
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

			ctx := CtxWithOriginalMessage(msg.Context(), msg)
			msg.SetContext(ctx)

			if err := p.config.Marshaler.Unmarshal(msg, event); err != nil {
				return err
			}

			handle := func(params EventGroupProcessorOnHandleParams) error {
				return params.Handler.Handle(ctx, params.Event)
			}
			if p.config.OnHandle != nil {
				handle = p.config.OnHandle
			}

			err := handle(EventGroupProcessorOnHandleParams{
				GroupName: groupName,
				Handler:   handler,
				EventName: messageEventName,
				Event:     event,
				Message:   msg,
			})
			if err != nil {
				logger.Debug("Error when handling event", watermill.LogFields{"err": err})
				return err
			}

			return nil
		}

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
