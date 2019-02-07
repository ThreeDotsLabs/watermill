package cqrs_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nonPointerCommandHandler struct {
}

func (nonPointerCommandHandler) NewCommand() interface{} {
	return TestCommand{}
}

func (nonPointerCommandHandler) Handle(cmd interface{}) error {
	panic("not implemented")
}

func TestCommandProcessor_non_pointer_command(t *testing.T) {
	ts := NewTestServices()

	commandProcessor := cqrs.NewCommandProcessor(
		[]cqrs.CommandHandler{nonPointerCommandHandler{}},
		"commands",
		ts.CommandsPubSub,
		ts.Marshaler,
		ts.Logger,
	)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = commandProcessor.AddHandlersToRouter(router)
	assert.IsType(t, cqrs.NonPointerError{}, errors.Cause(err))
}

// TestCommandProcessor_multiple_same_command_handlers checks, that we don't register multiple handlers for the same commend.
func TestCommandProcessor_multiple_same_command_handlers(t *testing.T) {
	ts := NewTestServices()

	commandProcessor := cqrs.NewCommandProcessor(
		[]cqrs.CommandHandler{
			&CaptureCommandHandler{},
			&CaptureCommandHandler{},
		},
		"commands",
		ts.CommandsPubSub,
		ts.Marshaler,
		ts.Logger,
	)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	assert.PanicsWithValue(t,
		message.DuplicateHandlerNameError{HandlerName: "command_processor-cqrs_test.TestCommand"},
		func() {
			err := commandProcessor.AddHandlersToRouter(router)
			require.NoError(t, err)
		},
	)
}
