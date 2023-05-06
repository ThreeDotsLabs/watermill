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

	NewCommand() interface{}

	Handle(ctx context.Context, cmd interface{}) error
}

type genericCommandHandler[T any] struct {
	handleFunc  func(ctx context.Context, cmd *T) error
	handlerName string
}

func NewCommandHandler[T any](handlerName string, handleFunc func(ctx context.Context, cmd *T) error) CommandHandler {
	return &genericCommandHandler[T]{
		handleFunc:  handleFunc,
		handlerName: handlerName,
	}
}

func (c genericCommandHandler[T]) HandlerName() string {
	return c.handlerName
}

func (c genericCommandHandler[T]) NewCommand() interface{} {
	tVar := new(T)
	return tVar
}

func (c genericCommandHandler[T]) Handle(ctx context.Context, cmd any) error {
	command := cmd.(*T)
	return c.handleFunc(ctx, command)
}
