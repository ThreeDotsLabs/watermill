package cqrs_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventProcessor(t *testing.T) {
	eventConfig := cqrs.EventConfig{
		GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
			return "", nil
		},
		SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
			return nil, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	}
	require.NoError(t, eventConfig.ValidateForProcessor())

	cp, err := cqrs.NewEventProcessorWithConfig(eventConfig)
	assert.NotNil(t, cp)
	assert.NoError(t, err)

	eventConfig.SubscriberConstructor = nil
	require.Error(t, eventConfig.ValidateForProcessor())

	cp, err = cqrs.NewEventProcessorWithConfig(eventConfig)
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

	handler := nonPointerEventProcessor{}

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(
		cqrs.EventConfig{
			GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
				return "", nil
			},
			SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
				return nil, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	eventProcessor.AddHandler(handler)

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

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(
		cqrs.EventConfig{
			GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
				return "", nil
			},
			SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
				return nil, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	eventProcessor.AddHandler(
		&duplicateTestEventHandler1{},
		&duplicateTestEventHandler2{},
	)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = eventProcessor.AddHandlersToRouter(router)
	require.NoError(t, err)
}

func TestNewEventProcessor_OnHandle(t *testing.T) {
	ts := NewTestServices()

	msg1, err := ts.Marshaler.Marshal(&TestEvent{ID: "1"})
	require.NoError(t, err)

	msg2, err := ts.Marshaler.Marshal(&TestEvent{ID: "2"})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg1,
			msg2,
		},
	}

	handlerCalled := 0

	defer func() {
		// for msg 1 we are not calling handler - but returning before
		assert.Equal(t, 1, handlerCalled)
	}()

	handler := cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
		handlerCalled++
		return nil
	})

	onHandleCalled := 0

	config := cqrs.EventConfig{
		GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
			return "events", nil
		},
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return "events", nil
		},
		SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
			return mockSub, nil
		},
		OnHandle: func(params cqrs.OnEventHandleParams) error {
			onHandleCalled++

			assert.IsType(t, &TestEvent{}, params.Event)
			assert.Equal(t, handler, params.Handler)

			if params.Event.(*TestEvent).ID == "1" {
				assert.Equal(t, msg1, params.Message)
				return errors.New("test error")
			} else {
				assert.Equal(t, msg2, params.Message)
			}

			return params.Handler.Handle(params.Message.Context(), params.Event)
		},
		Marshaler: ts.Marshaler,
		Logger:    ts.Logger,
	}
	cp, err := cqrs.NewEventProcessorWithConfig(config)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	cp.AddHandler(handler)

	err = cp.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msg1.Nacked():
		// ok
	case <-msg1.Acked():
		// ack received
		t.Fatal("ack received, message should be nacked")
	}

	select {
	case <-msg2.Acked():
		// ok
	case <-msg2.Nacked():
		// nack received
	}

	assert.Equal(t, 2, onHandleCalled)
}

type UnknownEvent struct {
}

func TestNewEventProcessor_AckOnUnknownEvent(t *testing.T) {
	ts := NewTestServices()

	msg, err := ts.Marshaler.Marshal(&UnknownEvent{})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg,
		},
	}

	cp, err := cqrs.NewEventProcessorWithConfig(
		cqrs.EventConfig{
			GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			AckOnUnknownEvent: true,
			Marshaler:         ts.Marshaler,
			Logger:            ts.Logger,
		},
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	cp.AddHandler(cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
		return nil
	}))

	err = cp.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msg.Acked():
		// ok
	case <-msg.Nacked():
		// ack received
		t.Fatal("ack received, message should be nacked")
	}
}

func TestNewEventProcessor_AckOnUnknownEvent_handler_group(t *testing.T) {
	ts := NewTestServices()

	msg, err := ts.Marshaler.Marshal(&UnknownEvent{})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg,
		},
	}

	cp, err := cqrs.NewEventProcessorWithConfig(
		cqrs.EventConfig{
			GenerateHandlerGroupSubscribeTopic: func(params cqrs.GenerateEventHandlerGroupTopicParams) (string, error) {
				return "events", nil
			},
			GroupSubscriberConstructor: func(params cqrs.EventsGroupSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			AckOnUnknownEvent: true,
			Marshaler:         ts.Marshaler,
			Logger:            ts.Logger,
		},
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = cp.AddHandlersGroup(
		"foo",
		cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
			return nil
		}),
	)
	require.NoError(t, err)

	err = cp.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msg.Acked():
		// ok
	case <-msg.Nacked():
		// ack received
		t.Fatal("ack received, message should be nacked")
	}
}

func TestNewEventProcessor_AckOnUnknownEvent_disabled_handler_group(t *testing.T) {
	ts := NewTestServices()

	msg, err := ts.Marshaler.Marshal(&UnknownEvent{})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg,
		},
	}

	cp, err := cqrs.NewEventProcessorWithConfig(
		cqrs.EventConfig{
			GenerateHandlerGroupSubscribeTopic: func(params cqrs.GenerateEventHandlerGroupTopicParams) (string, error) {
				return "events", nil
			},
			GroupSubscriberConstructor: func(params cqrs.EventsGroupSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			AckOnUnknownEvent: false,
			Marshaler:         ts.Marshaler,
			Logger:            ts.Logger,
		},
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = cp.AddHandlersGroup(
		"foo",
		cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
			return nil
		}),
	)
	require.NoError(t, err)

	err = cp.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msg.Nacked():
		// ok
	case <-msg.Acked():
		t.Fatal("ack received, message should be nacked")
	}
}

func TestNewEventProcessor_AckOnUnknownEvent_disabled(t *testing.T) {
	ts := NewTestServices()

	msg, err := ts.Marshaler.Marshal(&UnknownEvent{})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg,
		},
	}

	cp, err := cqrs.NewEventProcessorWithConfig(
		cqrs.EventConfig{
			GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			AckOnUnknownEvent: false,
			Marshaler:         ts.Marshaler,
			Logger:            ts.Logger,
		},
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	cp.AddHandler(cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
		return nil
	}))

	err = cp.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msg.Nacked():
		// ok
	case <-msg.Acked():
		// ack received
		t.Fatal("ack received, message should be nacked")
	}
}

func TestNewEventProcessor_backward_compatibility_of_AckOnUnknownEvent(t *testing.T) {
	ts := NewTestServices()

	msg, err := ts.Marshaler.Marshal(&UnknownEvent{})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg,
		},
	}

	cp, err := cqrs.NewEventProcessor(
		[]cqrs.EventHandler{
			cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
				return nil
			}),
		},
		func(eventName string) string {
			return "events"
		},
		func(handlerName string) (message.Subscriber, error) {
			return mockSub, nil
		},
		ts.Marshaler,
		ts.Logger,
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = cp.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msg.Acked():
		// ok
	case <-msg.Nacked():
		// ack received
		t.Fatal("ack received, message should be nacked")
	}
}

func TestEventProcessor_AddHandlersToRouter_missing_handlers(t *testing.T) {
	ts := NewTestServices()

	cp, err := cqrs.NewEventProcessorWithConfig(cqrs.EventConfig{
		GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
			return "", nil
		},
		SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
			return nil, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	})
	assert.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = cp.AddHandlersToRouter(router)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "EventProcessor has no handlers, did you call AddHandler or AddHandlersGroup?")
}

func TestEventProcessor_handler_group(t *testing.T) {
	ts := NewTestServices()

	event1 := &TestEvent{ID: "1"}

	msg1, err := ts.Marshaler.Marshal(event1)
	require.NoError(t, err)

	event2 := &AnotherTestEvent{ID: "2"}

	msg2, err := ts.Marshaler.Marshal(event2)
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg1,
			msg2,
		},
	}

	handler1Calls := 0
	handler2Calls := 0

	handlers := []cqrs.GroupEventHandler{
		cqrs.NewGroupEventHandler(func(ctx context.Context, event *TestEvent) error {
			assert.EqualValues(t, event1, event)

			handler1Calls++

			return nil
		}),
		cqrs.NewGroupEventHandler(func(ctx context.Context, event *AnotherTestEvent) error {
			assert.EqualValues(t, event2, event)

			handler2Calls++

			return nil
		}),
	}

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(
		cqrs.EventConfig{
			GenerateHandlerGroupSubscribeTopic: func(params cqrs.GenerateEventHandlerGroupTopicParams) (string, error) {
				assert.Equal(t, "some_group", params.EventGroupName)
				assert.Equal(t, handlers, params.EventGroupHandlers)

				return "events", nil
			},
			GroupSubscriberConstructor: func(params cqrs.EventsGroupSubscriberConstructorParams) (message.Subscriber, error) {
				assert.Equal(t, "some_group", params.EventGroupName)
				assert.Equal(t, handlers, params.EventGroupHandlers)

				return mockSub, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	err = eventProcessor.AddHandlersGroup(
		"some_group",
		handlers...,
	)
	require.NoError(t, err)

	err = eventProcessor.AddHandlersGroup(
		"some_group",
		handlers...,
	)
	require.ErrorContains(t, err, "event handler group 'some_group' already exists")

	err = eventProcessor.AddHandlersGroup(
		"some_group_2",
	)
	require.ErrorContains(t, err, "no handlers provided")

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = eventProcessor.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msg1.Acked():
	// ok
	case <-time.After(time.Second):
		t.Fatal("message 1 not acked")
	}

	select {
	case <-msg2.Acked():
	// ok
	case <-time.After(time.Second):
		t.Fatal("message 2 not acked")
	}

	assert.Equal(t, 1, handler1Calls)
	assert.Equal(t, 1, handler2Calls)
}
