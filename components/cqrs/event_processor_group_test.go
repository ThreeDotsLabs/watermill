package cqrs_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestEventGroupProcessorConfig_Validate(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*cqrs.EventGroupProcessorConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name:        "valid_with_group_handlers",
			ExpectedErr: nil,
		},
		{
			Name: "missing_GroupSubscriberConstructor",
			ModifyValidConfig: func(config *cqrs.EventGroupProcessorConfig) {
				config.SubscriberConstructor = nil
			},
			ExpectedErr: fmt.Errorf("missing SubscriberConstructor"),
		},
		{
			Name: "missing_GenerateHandlerGroupSubscribeTopic",
			ModifyValidConfig: func(config *cqrs.EventGroupProcessorConfig) {
				config.GenerateSubscribeTopic = nil
			},
			ExpectedErr: fmt.Errorf("missing GenerateHandlerGroupTopic"),
		},
		{
			Name: "missing_marshaler",
			ModifyValidConfig: func(config *cqrs.EventGroupProcessorConfig) {
				config.Marshaler = nil
			},
			ExpectedErr: fmt.Errorf("missing Marshaler"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := cqrs.EventGroupProcessorConfig{
				GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
					return "", nil
				},
				SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
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

func TestNewEventProcessor_OnGroupHandle(t *testing.T) {
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

	onHandleCalled := int64(0)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	config := cqrs.EventGroupProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
			return "events", nil
		},
		SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return mockSub, nil
		},
		OnHandle: func(params cqrs.EventGroupProcessorOnHandleParams) error {
			atomic.AddInt64(&onHandleCalled, 1)

			assert.Equal(t, "some_group", params.GroupName)

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
	cp, err := cqrs.NewEventGroupProcessorWithConfig(router, config)
	require.NoError(t, err)

	err = cp.AddHandlersGroup("some_group", handler)
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

	assert.EqualValues(t, 2, onHandleCalled)
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

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	cp, err := cqrs.NewEventGroupProcessorWithConfig(
		router,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			AckOnUnknownEvent: true,
			Marshaler:         ts.Marshaler,
			Logger:            ts.Logger,
		},
	)
	require.NoError(t, err)

	err = cp.AddHandlersGroup(
		"foo",
		cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
			return nil
		}),
	)
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

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	cp, err := cqrs.NewEventGroupProcessorWithConfig(
		router,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			AckOnUnknownEvent: false,
			Marshaler:         ts.Marshaler,
			Logger:            ts.Logger,
		},
	)
	require.NoError(t, err)

	err = cp.AddHandlersGroup(
		"foo",
		cqrs.NewEventHandler("test", func(ctx context.Context, cmd *TestEvent) error {
			return nil
		}),
	)
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

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	eventProcessor, err := cqrs.NewEventGroupProcessorWithConfig(
		router,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				assert.Equal(t, "some_group", params.EventGroupName)
				assert.Equal(t, handlers, params.EventGroupHandlers)

				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
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

func TestEventGroupProcessor_original_msg_set_to_ctx(t *testing.T) {
	ts := NewTestServices()

	msg, err := ts.Marshaler.Marshal(&TestEvent{})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg,
		},
	}

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	cp, err := cqrs.NewEventGroupProcessorWithConfig(
		router,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			AckOnUnknownEvent: true,
			Marshaler:         ts.Marshaler,
			Logger:            ts.Logger,
		},
	)
	require.NoError(t, err)

	var msgFromCtx *message.Message

	err = cp.AddHandlersGroup(
		"some_group",
		cqrs.NewGroupEventHandler(
			func(ctx context.Context, cmd *TestEvent) error {
				msgFromCtx = cqrs.OriginalMessageFromCtx(ctx)
				return nil
			}),
	)
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
		// nack received
		t.Fatal("nack received, message should be acked")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	require.NotNil(t, msgFromCtx)
	assert.Equal(t, msg, msgFromCtx)
}
