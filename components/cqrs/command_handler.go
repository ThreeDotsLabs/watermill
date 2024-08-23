package cqrs

import (
	"context"
)

// CommandHandler receives a command defined by NewCommand and handles it with the Handle method.
// If using DDD, CommandHandler may modify and persist the aggregate.
//
// In contrast to EventHandler, every Command must have only one CommandHandler.
//
// One instance of CommandHandler is used during handling messages.
// When multiple commands are delivered at the same time, Handle method can be executed multiple times at the same time.
// Because of that, Handle method needs to be thread safe!
type CommandHandler interface {
	// HandlerName is the name used in message.Router while creating handler.
	//
	// It will be also passed to CommandsSubscriberConstructor.
	// May be useful, for example, to create a consumer group per each handler.
	//
	// WARNING: If HandlerName was changed and is used for generating consumer groups,
	// it may result with **reconsuming all messages**!
	HandlerName() string

	NewCommand() any

	Handle(ctx context.Context, cmd any) error
}

type genericCommandHandler[Command any] struct {
	handleFunc  func(ctx context.Context, cmd *Command) error
	handlerName string
}

// NewCommandHandler creates a new CommandHandler implementation based on provided function
// and command type inferred from function argument.
func NewCommandHandler[Command any](
	handlerName string,
	handleFunc func(ctx context.Context, cmd *Command) error,
) CommandHandler {
	return &genericCommandHandler[Command]{
		handleFunc:  handleFunc,
		handlerName: handlerName,
	}
}

func (c genericCommandHandler[Command]) HandlerName() string {
	return c.handlerName
}

func (c genericCommandHandler[Command]) NewCommand() any {
	tVar := new(Command)
	return tVar
}

func (c genericCommandHandler[Command]) Handle(ctx context.Context, cmd any) error {
	command := cmd.(*Command)
	return c.handleFunc(ctx, command)
}
