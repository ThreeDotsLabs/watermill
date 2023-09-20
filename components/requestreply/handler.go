package requestreply

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type genericCommandHandler[Command any] struct {
	handleFunc  func(ctx context.Context, cmd *Command) error
	handlerName string
}

// NewCommandHandler creates a new CommandHandler implementation based on provided function
// and command type inferred from function argument.
func NewCommandHandler[Command any](
	handlerName string,
	backend Backend[struct{}],
	handleFunc func(ctx context.Context, cmd *Command) error,
) cqrs.CommandHandler {
	return cqrs.NewCommandHandler(handlerName, func(ctx context.Context, cmd *Command) error {
		handlerErr := handleFunc(ctx, cmd)

		// todo: test and describe logic that handlerErr is ignored
		return backend.OnCommandProcessed(ctx, OnCommandProcessedParams[struct{}]{
			Cmd:       cmd,
			CmdMsg:    cqrs.OriginalMessageFromCtx(ctx),
			HandleErr: handlerErr,
		})
	})
}

// todo: not needed? or keep that (as we don't recommend it in general? but it's not cqrs so whatever)
func NewCommandHandlerWithResponse[Command any, Response any](
	handlerName string,
	backend Backend[Response],
	handleFunc func(ctx context.Context, cmd *Command) (Response, error),
) cqrs.CommandHandler {
	return cqrs.NewCommandHandler(handlerName, func(ctx context.Context, cmd *Command) error {
		resp, handlerErr := handleFunc(ctx, cmd)

		// todo: test and describe logic that handlerErr is ignored
		return backend.OnCommandProcessed(ctx, OnCommandProcessedParams[Response]{
			Cmd:             cmd,
			CmdMsg:          cqrs.OriginalMessageFromCtx(ctx),
			HandlerResponse: resp,
			HandleErr:       handlerErr,
		})
	})
}
