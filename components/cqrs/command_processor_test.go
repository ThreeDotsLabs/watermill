package cqrs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandProcessorConfig_Validate(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*cqrs.CommandProcessorConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name: "missing_Marshaler",
			ModifyValidConfig: func(c *cqrs.CommandProcessorConfig) {
				c.Marshaler = nil
			},
			ExpectedErr: errors.New("missing Marshaler"),
		},
		{
			Name: "missing_SubscriberConstructor",
			ModifyValidConfig: func(c *cqrs.CommandProcessorConfig) {
				c.SubscriberConstructor = nil
			},
			ExpectedErr: errors.New("missing SubscriberConstructor"),
		},
		{
			Name: "missing_GenerateHandlerSubscribeTopic",
			ModifyValidConfig: func(c *cqrs.CommandProcessorConfig) {
				c.GenerateSubscribeTopic = nil
			},
			ExpectedErr: errors.New("missing GenerateSubscribeTopic"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := cqrs.CommandProcessorConfig{
				GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
					return "", nil
				},
				SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
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

func TestNewCommandProcessor(t *testing.T) {
	config := cqrs.CommandProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
			return "", nil
		},
		SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return nil, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	}
	require.NoError(t, config.Validate())

	router, err := message.NewRouter(message.RouterConfig{}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)

	cp, err := cqrs.NewCommandProcessorWithConfig(router, config)
	assert.NotNil(t, cp)
	assert.NoError(t, err)

	config.SubscriberConstructor = nil
	require.Error(t, config.Validate())

	cp, err = cqrs.NewCommandProcessorWithConfig(router, config)
	assert.Nil(t, cp)
	assert.Error(t, err)
}

type nonPointerCommandHandler struct {
}

func (nonPointerCommandHandler) HandlerName() string {
	return "nonPointerCommandHandler"
}

func (nonPointerCommandHandler) NewCommand() interface{} {
	return TestCommand{}
}

func (nonPointerCommandHandler) Handle(ctx context.Context, cmd interface{}) error {
	panic("not implemented")
}

func TestCommandProcessor_non_pointer_command(t *testing.T) {
	ts := NewTestServices()

	handler := nonPointerCommandHandler{}

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "", nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return nil, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	err = commandProcessor.AddHandlers(handler)
	assert.ErrorAs(t, err, &cqrs.NonPointerError{})
}

// TestCommandProcessor_multiple_same_command_handlers checks, that we don't register multiple handlers for the same command.
func TestCommandProcessor_multiple_same_command_handlers(t *testing.T) {
	ts := NewTestServices()

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "", nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return nil, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	err = commandProcessor.AddHandlers(
		&CaptureCommandHandler{},
		&CaptureCommandHandler{},
	)
	require.Error(t, err)
	assert.EqualValues(t, cqrs.DuplicateCommandHandlerError{CommandName: "cqrs_test.TestCommand"}, err)
	assert.Equal(t, "command handler for command cqrs_test.TestCommand already exists", err.Error())
}

type mockSubscriber struct {
	MessagesToSend []*message.Message
	out            chan *message.Message
}

func (m *mockSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	m.out = make(chan *message.Message)

	go func() {
		for _, msg := range m.MessagesToSend {
			m.out <- msg
		}
	}()

	return m.out, nil
}

func (m mockSubscriber) Close() error {
	close(m.out)
	return nil
}

func TestCommandProcessor_AckCommandHandlingErrors_option_true(t *testing.T) {
	logger := watermill.NewCaptureLogger()

	marshaler := cqrs.JSONMarshaler{}

	msgToSend, err := marshaler.Marshal(&TestCommand{ID: "1"})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msgToSend,
		},
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "commands", nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			Marshaler:                marshaler,
			Logger:                   logger,
			AckCommandHandlingErrors: true,
		},
	)
	require.NoError(t, err)

	expectedErr := errors.New("test error")

	err = commandProcessor.AddHandlers(cqrs.NewCommandHandler(
		"handler", func(ctx context.Context, cmd *TestCommand) error {
			return expectedErr
		}),
	)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msgToSend.Acked():
		// ok
	case <-msgToSend.Nacked():
		// nack received
		t.Fatal("nack received, message should be acked")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	// it's pretty important to not ack message silently, so let's assert if it's logged properly
	expectedLogMessage := watermill.CapturedMessage{
		Level: watermill.ErrorLogLevel,
		Fields: map[string]any{
			"command_handler_name": "handler",
			"topic":                "commands",
		},
		Msg: "Error when handling command, acking (AckCommandHandlingErrors is enabled)",
		Err: expectedErr,
	}
	assert.True(
		t,
		logger.Has(expectedLogMessage),
		"expected log message not found, logs: %#v",
		logger.Captured(),
	)
}

func TestCommandProcessor_AckCommandHandlingErrors_option_false(t *testing.T) {
	logger := watermill.NewCaptureLogger()

	marshaler := cqrs.JSONMarshaler{}

	msgToSend, err := marshaler.Marshal(&TestCommand{ID: "1"})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msgToSend,
		},
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "commands", nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			Marshaler:                marshaler,
			Logger:                   logger,
			AckCommandHandlingErrors: false,
		},
	)
	require.NoError(t, err)

	expectedErr := errors.New("test error")

	err = commandProcessor.AddHandlers(cqrs.NewCommandHandler(
		"handler", func(ctx context.Context, cmd *TestCommand) error {
			return expectedErr
		}),
	)
	require.NoError(t, err)

	go func() {
		err := router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	select {
	case <-msgToSend.Acked():
		// nack received
		t.Fatal("ack received, message should be nacked")
	case <-msgToSend.Nacked():
		// ok
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for ack")
	}
}

func TestNewCommandProcessor_OnHandle(t *testing.T) {
	ts := NewTestServices()

	msg1, err := ts.Marshaler.Marshal(&TestCommand{ID: "1"})
	require.NoError(t, err)

	msg2, err := ts.Marshaler.Marshal(&TestCommand{ID: "2"})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msg1,
			msg2,
		},
	}

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	handlerCalled := 0

	defer func() {
		// for msg 1 we are not calling handler - but returning before
		assert.Equal(t, 1, handlerCalled)
	}()

	handler := cqrs.NewCommandHandler("test", func(ctx context.Context, cmd *TestCommand) error {
		handlerCalled++
		return nil
	})

	onHandleCalled := int64(0)

	config := cqrs.CommandProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
			return "commands", nil
		},
		SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return mockSub, nil
		},
		OnHandle: func(params cqrs.CommandProcessorOnHandleParams) error {
			atomic.AddInt64(&onHandleCalled, 1)

			assert.IsType(t, &TestCommand{}, params.Command)
			assert.Equal(t, "cqrs_test.TestCommand", params.CommandName)
			assert.Equal(t, handler, params.Handler)

			if params.Command.(*TestCommand).ID == "1" {
				assert.Equal(t, msg1, params.Message)
				return errors.New("test error")
			} else {
				assert.Equal(t, msg2, params.Message)
			}

			return params.Handler.Handle(params.Message.Context(), params.Command)
		},
		Marshaler: ts.Marshaler,
		Logger:    ts.Logger,
	}
	cp, err := cqrs.NewCommandProcessorWithConfig(router, config)
	require.NoError(t, err)

	err = cp.AddHandlers(handler)
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

func TestCommandProcessor_AddHandlersToRouter_without_disableRouterAutoAddHandlers(t *testing.T) {
	ts := NewTestServices()

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	cp, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "commands", nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return ts.CommandsPubSub, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	err = cp.AddHandlersToRouter(router)
	assert.ErrorContains(t, err, "AddHandlersToRouter should be called only when using deprecated NewCommandProcessor")
}

func TestCommandProcessor_original_msg_set_to_ctx(t *testing.T) {
	logger := watermill.NewCaptureLogger()

	marshaler := cqrs.JSONMarshaler{}

	msgToSend, err := marshaler.Marshal(&TestCommand{ID: "1"})
	require.NoError(t, err)

	mockSub := &mockSubscriber{
		MessagesToSend: []*message.Message{
			msgToSend,
		},
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "commands", nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			Marshaler:                marshaler,
			Logger:                   logger,
			AckCommandHandlingErrors: true,
		},
	)
	require.NoError(t, err)

	var msgFromCtx *message.Message

	err = commandProcessor.AddHandlers(cqrs.NewCommandHandler(
		"handler", func(ctx context.Context, cmd *TestCommand) error {
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
	case <-msgToSend.Acked():
		// ok
	case <-msgToSend.Nacked():
		// nack received
		t.Fatal("nack received, message should be acked")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	require.NotNil(t, msgFromCtx)
	assert.Equal(t, msgToSend, msgFromCtx)
}
