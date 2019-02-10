package cqrs

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type EventHandler interface {
	NewEvent() interface{}
	Handle(event interface{}) error
}

type EventProcessor struct {
	handlers    []EventHandler
	eventsTopic string

	subscriber message.Subscriber
	marshaler  CommandEventMarshaler
	logger     watermill.LoggerAdapter
}

func NewEventProcessor(
	handlers []EventHandler,
	eventsTopic string,
	subscriber message.Subscriber,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
) *EventProcessor {
	if len(handlers) == 0 {
		panic("missing handlers")
	}
	if eventsTopic == "" {
		panic("empty eventsTopic")
	}
	if subscriber == nil {
		panic("missing subscriber")
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
		subscriber,
		marshaler,
		logger,
	}
}

func (p EventProcessor) AddHandlersToRouter(r *message.Router) error {
	for i := range p.Handlers() {
		handler := p.handlers[i]

		handlerFunc, err := p.RouterHandlerFunc(handler)
		if err != nil {
			return err
		}

		r.AddNoPublisherHandler(
			fmt.Sprintf("event_processor-%s", ObjectName(handler)),
			p.eventsTopic,
			p.subscriber,
			handlerFunc,
		)
	}

	return nil
}

func (p EventProcessor) Handlers() []EventHandler {
	return p.handlers
}

func (p EventProcessor) RouterHandlerFunc(handler EventHandler) (message.HandlerFunc, error) {
	initEvent := handler.NewEvent()
	expectedEventName := p.marshaler.Name(initEvent)

	if err := p.validateEvent(initEvent); err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		event := handler.NewEvent()
		messageEventName := p.marshaler.NameFromMessage(msg)

		if messageEventName != expectedEventName {
			p.logger.Trace("Received different event type than expected, ignoring", watermill.LogFields{
				"message_uuid":        msg.UUID,
				"expected_event_type": expectedEventName,
				"received_event_type": messageEventName,
			})
			return nil, nil
		}

		p.logger.Debug("Handling event", watermill.LogFields{
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
