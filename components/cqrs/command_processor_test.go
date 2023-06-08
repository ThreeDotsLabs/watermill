package cqrs_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCommandProcessor(t *testing.T) {
	handlers := []cqrs.CommandHandler{nonPointerCommandHandler{}}

	generateTopic := func(commandName string) string {
		return ""
	}
	subscriberConstructor := func(handlerName string) (subscriber message.Subscriber, e error) {
		return nil, nil
	}

	cp, err := cqrs.NewCommandProcessor(handlers, generateTopic, subscriberConstructor, cqrs.JSONMarshaler{}, nil)
	assert.NotNil(t, cp)
	assert.NoError(t, err)

	cp, err = cqrs.NewCommandProcessor([]cqrs.CommandHandler{}, generateTopic, subscriberConstructor, cqrs.JSONMarshaler{}, nil)
	assert.Nil(t, cp)
	assert.Error(t, err)

	cp, err = cqrs.NewCommandProcessor(handlers, nil, subscriberConstructor, cqrs.JSONMarshaler{}, nil)
	assert.Nil(t, cp)
	assert.Error(t, err)

	cp, err = cqrs.NewCommandProcessor(handlers, generateTopic, nil, cqrs.JSONMarshaler{}, nil)
	assert.Nil(t, cp)
	assert.Error(t, err)

	cp, err = cqrs.NewCommandProcessor(handlers, generateTopic, subscriberConstructor, nil, nil)
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

	commandProcessor, err := cqrs.NewCommandProcessor(
		[]cqrs.CommandHandler{nonPointerCommandHandler{}},
		func(commandName string) string {
			return "commands"
		},
		func(handlerName string) (message.Subscriber, error) {
			return ts.CommandsPubSub, nil
		},
		ts.Marshaler,
		ts.Logger,
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = commandProcessor.AddHandlersToRouter(router)
	assert.IsType(t, cqrs.NonPointerError{}, errors.Cause(err))
}

// TestCommandProcessor_multiple_same_command_handlers checks, that we don't register multiple handlers for the same commend.
func TestCommandProcessor_multiple_same_command_handlers(t *testing.T) {
	ts := NewTestServices()

	commandProcessor, err := cqrs.NewCommandProcessor(
		[]cqrs.CommandHandler{
			&CaptureCommandHandler{},
			&CaptureCommandHandler{},
		},
		func(commandName string) string {
			return "commands"
		},
		func(handlerName string) (message.Subscriber, error) {
			return ts.CommandsPubSub, nil
		},
		ts.Marshaler,
		ts.Logger,
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = commandProcessor.AddHandlersToRouter(router)
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

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		cqrs.CommandConfig{
			GenerateBusTopic: func(params cqrs.GenerateCommandBusTopicParams) (string, error) {
				return "commands", nil
			},
			GenerateHandlerTopic: func(params cqrs.GenerateCommandHandlerTopicParams) (string, error) {
				return "commands", nil
			},
			SubscriberConstructor: func(params cqrs.CommandsSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			Marshaler:                marshaler,
			Logger:                   logger,
			AckCommandHandlingErrors: true,
		},
	)
	require.NoError(t, err)

	expectedErr := errors.New("test error")

	commandProcessor.AddHandler(cqrs.NewCommandHandler(
		"handler", func(ctx context.Context, cmd *TestCommand) error {
			return expectedErr
		}),
	)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	err = commandProcessor.AddHandlersToRouter(router)
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
		Msg: "Error when handling command",
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

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		cqrs.CommandConfig{
			GenerateBusTopic: func(params cqrs.GenerateCommandBusTopicParams) (string, error) {
				return "commands", nil
			},
			GenerateHandlerTopic: func(params cqrs.GenerateCommandHandlerTopicParams) (string, error) {
				return "commands", nil
			},
			SubscriberConstructor: func(params cqrs.CommandsSubscriberConstructorParams) (message.Subscriber, error) {
				return mockSub, nil
			},
			Marshaler:                marshaler,
			Logger:                   logger,
			AckCommandHandlingErrors: false,
		},
	)
	require.NoError(t, err)

	expectedErr := errors.New("test error")

	commandProcessor.AddHandler(cqrs.NewCommandHandler(
		"handler", func(ctx context.Context, cmd *TestCommand) error {
			return expectedErr
		}),
	)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	err = commandProcessor.AddHandlersToRouter(router)
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
