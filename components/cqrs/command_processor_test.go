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
}
