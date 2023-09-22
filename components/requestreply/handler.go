package requestreply

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// NewCommandHandler creates a new CommandHandler implementation based on provided function
// and command type inferred from function argument.
func NewCommandHandler[Command any](
	handlerName string,
	backend Backend[struct{}],
	handleFunc func(ctx context.Context, cmd *Command) error,
) cqrs.CommandHandler {
	return cqrs.NewCommandHandler(handlerName, func(ctx context.Context, cmd *Command) error {
		handlerErr := handleFunc(ctx, cmd)

		originalMessage, err := originalCommandMsgFromCtx(ctx)
		if err != nil {
			return err
		}

		// todo: test and describe logic that handlerErr is ignored
		return backend.OnCommandProcessed(ctx, BackendOnCommandProcessedParams[struct{}]{
			Command:        cmd,
			CommandMessage: originalMessage,
			HandleErr:      handlerErr,
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

		originalMessage, err := originalCommandMsgFromCtx(ctx)
		if err != nil {
			return err
		}

		// todo: test and describe logic that handlerErr is ignored
		return backend.OnCommandProcessed(ctx, BackendOnCommandProcessedParams[Response]{
			Command:         cmd,
			CommandMessage:  originalMessage,
			HandlerResponse: resp,
			HandleErr:       handlerErr,
		})
	})
}

func originalCommandMsgFromCtx(ctx context.Context) (*message.Message, error) {
	originalMessage := cqrs.OriginalMessageFromCtx(ctx)
	if originalMessage == nil {
		return nil, errors.New(
			"original message not found in context, did you used cqrs.CommandProcessor? " +
				"if you are using custom implementation, please call cqrs.CtxWithOriginalMessage on context passed to handler",
		)
	}
	return originalMessage, nil
}
