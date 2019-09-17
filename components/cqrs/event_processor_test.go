package cqrs_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventProcessor(t *testing.T) {
	handlers := []cqrs.EventHandler{nonPointerEventProcessor{}}

	generateTopic := func(commandName string) string {
		return ""
	}
	subscriberConstructor := func(handlerName string) (subscriber message.Subscriber, e error) {
		return nil, nil
	}

	cp, err := cqrs.NewEventProcessor(handlers, generateTopic, subscriberConstructor, cqrs.JSONMarshaler{}, nil)
	assert.NotNil(t, cp)
	assert.NoError(t, err)

	cp, err = cqrs.NewEventProcessor([]cqrs.EventHandler{}, generateTopic, subscriberConstructor, cqrs.JSONMarshaler{}, nil)
	assert.Nil(t, cp)
	assert.Error(t, err)

	cp, err = cqrs.NewEventProcessor(handlers, nil, subscriberConstructor, cqrs.JSONMarshaler{}, nil)
	assert.Nil(t, cp)
	assert.Error(t, err)

	cp, err = cqrs.NewEventProcessor(handlers, generateTopic, nil, cqrs.JSONMarshaler{}, nil)
	assert.Nil(t, cp)
	assert.Error(t, err)

	cp, err = cqrs.NewEventProcessor(handlers, generateTopic, subscriberConstructor, nil, nil)
	assert.Nil(t, cp)
	assert.Error(t, err)
}

type nonPointerEventProcessor struct {
}

func (nonPointerEventProcessor) HandlerName() string {
	return "nonPointerEventProcessor"
}

func (nonPointerEventProcessor) NewEvent() interface{} {
	return TestEvent{}
}

func (nonPointerEventProcessor) Handle(ctx context.Context, cmd interface{}) error {
	panic("not implemented")
}

func TestEventProcessor_non_pointer_event(t *testing.T) {
	ts := NewTestServices()

	eventProcessor, err := cqrs.NewEventProcessor(
		[]cqrs.EventHandler{nonPointerEventProcessor{}},
		func(eventName string) string {
			return "events"
		},
		func(handlerName string) (message.Subscriber, error) {
			return ts.EventsPubSub, nil
		},
		ts.Marshaler,
		ts.Logger,
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = eventProcessor.AddHandlersToRouter(router)
	assert.IsType(t, cqrs.NonPointerError{}, errors.Cause(err))
}

type duplicateTestEventHandler1 struct{}

func (h duplicateTestEventHandler1) HandlerName() string {
	return "duplicateTestEventHandler1"
}

func (duplicateTestEventHandler1) NewEvent() interface{} {
	return &TestEvent{}
}

func (h *duplicateTestEventHandler1) Handle(ctx context.Context, event interface{}) error { return nil }

type duplicateTestEventHandler2 struct{}

func (h duplicateTestEventHandler2) HandlerName() string {
	return "duplicateTestEventHandler2"
}

func (duplicateTestEventHandler2) NewEvent() interface{} {
	return &TestEvent{}
}

func (h *duplicateTestEventHandler2) Handle(ctx context.Context, event interface{}) error { return nil }

func TestEventProcessor_multiple_same_event_handlers(t *testing.T) {
	ts := NewTestServices()

	eventProcessor, err := cqrs.NewEventProcessor(
		[]cqrs.EventHandler{
			&duplicateTestEventHandler1{},
			&duplicateTestEventHandler2{},
		},
		func(eventName string) string {
			return "events"
		},
		func(handlerName string) (message.Subscriber, error) {
			return ts.EventsPubSub, nil
		},
		ts.Marshaler,
		ts.Logger,
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = eventProcessor.AddHandlersToRouter(router)
	require.NoError(t, err)
}
