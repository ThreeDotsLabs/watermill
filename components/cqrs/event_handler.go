package cqrs

import (
	"context"
)

// EventHandler receives events defined by NewEvent and handles them with its Handle method.
// If using DDD, CommandHandler may modify and persist the aggregate.
// It can also invoke a process manager, a saga or just build a read model.
//
// In contrast to CommandHandler, every Event can have multiple EventHandlers.
//
// One instance of EventHandler is used during handling messages.
// When multiple events are delivered at the same time, Handle method can be executed multiple times at the same time.
// Because of that, Handle method needs to be thread safe!
type EventHandler interface {
	// HandlerName is the name used in message.Router while creating handler.
	//
	// It will be also passed to EventsSubscriberConstructor.
	// May be useful, for example, to create a consumer group per each handler.
	//
	// WARNING: If HandlerName was changed and is used for generating consumer groups,
	// it may result with **reconsuming all messages** !!!
	HandlerName() string

	NewEvent() any

	Handle(ctx context.Context, event any) error
}

type genericEventHandler[T any] struct {
	handleFunc  func(ctx context.Context, cmd *T) error
	handlerName string
}

func NewEventHandler[T any](handlerName string, handleFunc func(ctx context.Context, cmd *T) error) EventHandler {
	return &genericEventHandler[T]{
		handleFunc:  handleFunc,
		handlerName: handlerName,
	}
}

func (c genericEventHandler[T]) HandlerName() string {
	return c.handlerName
}

func (c genericEventHandler[T]) NewEvent() any {
	tVar := new(T)
	return tVar
}

func (c genericEventHandler[T]) Handle(ctx context.Context, cmd any) error {
	event := cmd.(*T)
	return c.handleFunc(ctx, event)
}

type GroupEventHandler interface {
	NewEvent() interface{}
	Handle(ctx context.Context, event interface{}) error
}

// todo: test!
func NewGroupEventHandler[T any](handleFunc func(ctx context.Context, cmd *T) error) GroupEventHandler {
	return &genericEventHandler[T]{
		handleFunc: handleFunc,
	}
}
