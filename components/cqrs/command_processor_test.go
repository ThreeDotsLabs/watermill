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
