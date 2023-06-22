package cqrs_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventProcessorConfig_Validate(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*cqrs.EventProcessorConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name: "missing_GenerateHandlerSubscribeTopic",
			ModifyValidConfig: func(config *cqrs.EventProcessorConfig) {
				config.GenerateSubscribeTopic = nil
			},
			ExpectedErr: fmt.Errorf("missing GenerateHandlerTopic"),
		},
		{
			Name: "missing_marshaler",
			ModifyValidConfig: func(config *cqrs.EventProcessorConfig) {
				config.Marshaler = nil
			},
			ExpectedErr: fmt.Errorf("missing Marshaler"),
		},
		{
			Name: "missing_subscriber_constructor",
			ModifyValidConfig: func(config *cqrs.EventProcessorConfig) {
				config.SubscriberConstructor = nil
			},
			ExpectedErr: fmt.Errorf("missing SubscriberConstructor"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := cqrs.EventProcessorConfig{
				GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
					return "", nil
				},
				SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
					return nil, nil
				},
				Marshaler: cqrs.JSONMarshaler{},
			}

			if tc.ModifyValidConfig != nil {
				tc.ModifyValidConfig(&validConfig)
			}

			err := validConfig.Validate()
			if tc.ExpectedErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.ExpectedErr.Error())
			}
		})
	}
}

func TestNewEventProcessor(t *testing.T) {
	eventConfig := cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return "", nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return nil, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	}
	require.NoError(t, eventConfig.Validate())

	cp, err := cqrs.NewEventProcessorWithConfig(eventConfig)
	assert.NotNil(t, cp)
	assert.NoError(t, err)

	eventConfig.SubscriberConstructor = nil
	require.Error(t, eventConfig.Validate())

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
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return "", nil
			},
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return nil, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	eventProcessor.AddHandlers(handler)

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
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return "", nil
			},
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return nil, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	eventProcessor.AddHandlers(
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

	config := cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return "events", nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return mockSub, nil
		},
		OnHandle: func(params cqrs.EventProcessorOnHandleParams) error {
			onHandleCalled++

			assert.IsType(t, &TestEvent{}, params.Event)
			assert.Equal(t, "cqrs_test.TestEvent", params.EventName)
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

	cp.AddHandlers(handler)

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
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
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

	cp.AddHandlers(cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
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
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
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

	cp.AddHandlers(cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
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

	cp, err := cqrs.NewEventProcessorWithConfig(cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return "", nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return nil, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	})
	assert.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = cp.AddHandlersToRouter(router)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "EventProcessor has no handlers, did you call AddHandlers?")
}
