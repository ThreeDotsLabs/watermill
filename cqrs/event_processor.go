package cqrs

import (
	"fmt"
	"reflect"

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
	marshaler  Marshaler
	logger     watermill.LoggerAdapter
}

func NewEventProcessor(
	handlers []EventHandler,
	eventsTopic string,
	subscriber message.Subscriber,
	marshaler Marshaler,
	logger watermill.LoggerAdapter,
) EventProcessor {
	return EventProcessor{
		handlers,
		eventsTopic,
		subscriber,
		marshaler,
		logger,
	}
}

func (p EventProcessor) Handlers() []EventHandler {
	return p.handlers
}

func (p EventProcessor) AddHandlersToRouter(r *message.Router) error {
	for i := range p.Handlers() {
		handler := p.handlers[i]
		eventName := p.marshaler.Name(handler.NewEvent())

		handlerFunc, err := p.RouterHandlerFunc(handler)
		if err != nil {
			return err
		}

		if err := r.AddNoPublisherHandler(
			fmt.Sprintf("event_processor_%s", eventName),
			p.eventsTopic,
			p.subscriber,
			handlerFunc,
		); err != nil {
			return err
		}
	}

	return nil
}

// todo - deduplicate with command processor
func (p EventProcessor) RouterHandlerFunc(handler EventHandler) (message.HandlerFunc, error) {
	initEvent := handler.NewEvent()
	expectedEventName := p.marshaler.Name(initEvent)

	if err := p.validateEvent(initEvent); err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		event := handler.NewEvent()
		messageEventName := p.marshaler.MarshaledName(msg)

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
	rv := reflect.ValueOf(event)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return NonPointerEventError{rv.Type()}
	}

	return nil
}

type NonPointerEventError struct {
	Type reflect.Type
}

func (e NonPointerEventError) Error() string {
	return "non-pointer event: " + e.Type.String() + ", handler.NewEvent() should return pointer to the event"
}
