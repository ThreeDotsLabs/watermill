package cqrs

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// EventHandler receive event defined by NewEvent and handle it with Handle method.
// If using DDD, CommandHandler may modify and persist the aggregate.
// It can also invoke process manager, saga or just build a read model.
//
// In contrast to CommandHandler, every Event can have multiple EventHandlers.
type EventHandler interface {
	NewEvent() interface{}
	Handle(event interface{}) error
}

type EventsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// EventProcessor determines which EventHandler should handle event received from event bus.
type EventProcessor struct {
	handlers    []EventHandler
	eventsTopic string

	subscriberConstructor EventsSubscriberConstructor

	marshaler CommandEventMarshaler
	logger    watermill.LoggerAdapter
}

func NewEventProcessor(
	handlers []EventHandler,
	eventsTopic string,
	subscriberConstructor EventsSubscriberConstructor,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
) *EventProcessor {
	if len(handlers) == 0 {
		panic("missing handlers")
	}
	if eventsTopic == "" {
		panic("empty eventsTopic")
	}
	if subscriberConstructor == nil {
		panic("missing subscriberConstructor")
	}
	if marshaler == nil {
		panic("missing marshaler")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &EventProcessor{
		handlers,
		eventsTopic,
		subscriberConstructor,
		marshaler,
		logger,
	}
}

func (p EventProcessor) AddHandlersToRouter(r *message.Router) error {
	for i := range p.Handlers() {
		handler := p.handlers[i]

		handlerName := fmt.Sprintf("event_processor-%s", ObjectName(handler))

		logger := p.logger.With(watermill.LogFields{"handler_name": handlerName})

		handlerFunc, err := p.RouterHandlerFunc(handler, logger)
		if err != nil {
			return err
		}

		// todo - should be passed implicit!
		// todo - doc it really well how it is executed
		subscriber, err := p.subscriberConstructor(handlerName)
		if err != nil {
			return errors.Wrap(err, "cannot create subscriber for event processor")
		}

		r.AddNoPublisherHandler(
			handlerName,
			p.eventsTopic,
			subscriber,
			handlerFunc,
		)
	}

	return nil
}

func (p EventProcessor) Handlers() []EventHandler {
	return p.handlers
}

func (p EventProcessor) RouterHandlerFunc(handler EventHandler, logger watermill.LoggerAdapter) (message.HandlerFunc, error) {
	initEvent := handler.NewEvent()
	expectedEventName := p.marshaler.Name(initEvent)

	if err := p.validateEvent(initEvent); err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		event := handler.NewEvent()
		messageEventName := p.marshaler.NameFromMessage(msg)

		if messageEventName != expectedEventName {
			logger.Trace("Received different event type than expected, ignoring", watermill.LogFields{
				"message_uuid":        msg.UUID,
				"expected_event_type": expectedEventName,
				"received_event_type": messageEventName,
			})
			return nil, nil
		}

		logger.Debug("Handling event", watermill.LogFields{
			"message_uuid":        msg.UUID,
			"received_event_type": messageEventName,
		})

		if err := p.marshaler.Unmarshal(msg, event); err != nil {
			return nil, err
		}

		if err := handler.Handle(event); err != nil {
			return nil, err
		}

		return nil, nil
	}, nil
}

func (p EventProcessor) validateEvent(event interface{}) error {
	// EventHandler's NewEvent must return a pointer, because it is used to unmarshal
	if err := isPointer(event); err != nil {
		return errors.Wrap(err, "command must be a non-nil pointer")
	}

	return nil
}
