package cqrs

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// EventsSubscriberConstructor creates a subscriber for EventHandler.
// It allows you to create separated customized Subscriber for every command handler.
//
// When handler groups are used, handler group is passed as handlerName.
type EventsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// EventProcessor determines which EventHandler should handle event received from event bus.
type EventProcessor struct {
	individualHandlers []EventHandler
	groupEventHandlers map[string][]GroupEventHandler

	config EventProcessorConfig
}

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

type EventProcessorConfig struct {
	GenerateIndividualSubscriberTopic GenerateEventsTopicFn
	GenerateHandlerGroupTopic         GenerateEventsGroupTopicFn

	ErrorOnUnknownEvent bool

	SubscriberConstructor EventsSubscriberConstructor

	Marshaler CommandEventMarshaler
	Logger    watermill.LoggerAdapter
}

func (c *EventProcessorConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
	if c.Marshaler == nil {
		c.Marshaler = JSONMarshaler{}
	}
}

func (c EventProcessorConfig) Validate() error {
	var err error

	if c.GenerateIndividualSubscriberTopic == nil && c.GenerateHandlerGroupTopic == nil {
		err = multierror.Append(err, errors.New("GenerateIndividualSubscriberTopic or GenerateHandlerGroupTopic must be set"))
	}

	if c.SubscriberConstructor == nil {
		err = multierror.Append(err, errors.New("missing SubscriberConstructor"))
	}

	return nil
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

	return &EventProcessor{
		individualHandlers: individualHandlers,
		groupEventHandlers: map[string][]GroupEventHandler{},
		config: EventProcessorConfig{
			GenerateIndividualSubscriberTopic: func(params GenerateEventsTopicParams) string {
				return generateTopic(params.EventName)
			},
			GenerateHandlerGroupTopic: nil,
			SubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
				return subscriberConstructor(handlerName)
			},
			Marshaler: marshaler,
		},
	}, nil
}

// NewEventProcessorWithConfig creates a new EventProcessor.
func NewEventProcessorWithConfig(config EventProcessorConfig) (*EventProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &EventProcessor{
		groupEventHandlers: map[string][]GroupEventHandler{},
		config:             config,
	}, nil
}

func (p *EventProcessor) AddHandler(handler ...EventHandler) *EventProcessor {
	p.individualHandlers = append(p.individualHandlers, handler...)
	return p
}

func (p *EventProcessor) AddHandlersGroup(handlerName string, handlers []GroupEventHandler) (*EventProcessor, error) {
	if len(handlers) == 0 {
		return nil, errors.New("missing handlers")
	}
	if _, ok := p.groupEventHandlers[handlerName]; ok {
		return nil, fmt.Errorf("event handler group '%s' already exists", handlerName)
	}

	p.groupEventHandlers[handlerName] = handlers

	return p, nil
}

func (p EventProcessor) AddHandlersToRouter(r *message.Router) error {
	handlers := p.Handlers()
	if len(handlers) == 0 {
		return errors.New("missing handlers")
	}

	for i := range p.individualHandlers {
		if p.config.GenerateIndividualSubscriberTopic == nil {
			return errors.New("missing GenerateIndividualSubscriberTopic configuration option")
		}

		handler := p.individualHandlers[i]

		if err := p.validateEvent(handler.NewEvent()); err != nil {
			return fmt.Errorf("invalid event for handler %s: %w", handler.HandlerName(), err)
		}

		handlerName := handler.HandlerName()
		eventName := p.config.Marshaler.Name(handler.NewEvent())
		topicName := p.config.GenerateIndividualSubscriberTopic(GenerateEventsTopicParams{
			EventName: eventName,
			Handler:   handler,
		})

		logger := p.config.Logger.With(watermill.LogFields{
			"event_handler_name": handlerName,
			"topic":              topicName,
		})

		handlerFunc, err := p.routerHandlerFunc(handler, logger)
		if err != nil {
			return err
		}

		if err := p.addHandlerToRouter(r, handlerName, topicName, handlerFunc); err != nil {
			return err
		}
	}

	for groupName := range p.groupEventHandlers {
		if p.config.GenerateHandlerGroupTopic == nil {
			return errors.New("missing GenerateHandlerGroupTopic configuration option")
		}

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

		topicName := p.config.GenerateHandlerGroupTopic(GenerateEventsGroupTopicParams{
			GroupName: groupName,
			Handlers:  handlersGroup,
		})

		logger := p.config.Logger.With(watermill.LogFields{
			"event_handler_group_name": groupName,
			"topic":                    topicName,
		})

		handlerFunc, err := p.routerHandlerGroupFunc(handlersGroup, logger)
		if err != nil {
			return err
		}

		if err := p.addHandlerToRouter(r, groupName, topicName, handlerFunc); err != nil {
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

func (p EventProcessor) addHandlerToRouter(
	r *message.Router,
	handlerName string,
	topicName string,
	handlerFunc message.NoPublishHandlerFunc,
) error {
	logger := p.config.Logger.With(watermill.LogFields{
		"event_handler_name": handlerName,
		"topic":              topicName,
	})

	logger.Debug("Adding CQRS event handler to router", nil)

	subscriber, err := p.config.SubscriberConstructor(handlerName)
	if err != nil {
		return errors.Wrap(err, "cannot create subscriber for event processor")
	}

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
			if p.config.ErrorOnUnknownEvent {
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

		if err := handler.Handle(msg.Context(), event); err != nil {
			logger.Debug("Error when handling event", watermill.LogFields{"err": err})
			return err
		}

		return nil
	}, nil
}

func (p EventProcessor) routerHandlerGroupFunc(handlers []GroupEventHandler, logger watermill.LoggerAdapter) (message.NoPublishHandlerFunc, error) {
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

			if err := handler.Handle(msg.Context(), event); err != nil {
				logger.Debug("Error when handling event", watermill.LogFields{"err": err})
				return err
			}

			return nil
		}

		if p.config.ErrorOnUnknownEvent {
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
