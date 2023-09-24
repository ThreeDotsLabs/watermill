package requestreply

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type CommandBus interface {
	SendWithModifiedMessage(ctx context.Context, cmd any, modify func(*message.Message) error) error
}

// SendWithReply sends a command to the command bus and receives replies from the command handler(s).
// It returns a channel with replies, cancel function, and error.
//
// SendWithReply can be canceled by calling the cancel function, canceling the context, or exceeding the timeout set in the backend (if set).
// When SendWithReply is canceled, the returned channel is closed as well.
// 
// Warning: It's important to cancel the function because it listens for the replies in the background.
// Not canceling it can lead to subscriber leaks.
//
// SendWithReply can listen for handlers with results (NewCommandHandlerWithResult) and without results (NewCommandHandler).
// When listening for handlers without results, you should pass `NoResult` or `struct{}` as the `Result` generic type:
//
//	 replyCh, cancel, err := requestreply.SendWithReply[requestreply.NoResult](
//			context.Background(),
//			ts.CommandBus,
//			ts.RequestReplyBackend,
//			&TestCommand{ID: "1"},
//		)
//
// If the `NewCommandHandlerWithResult` handler returns a specific type, you should pass it as the `Result` generic type:
//
//	 replyCh, cancel, err := requestreply.SendWithReply[SomeTypeReturnedByHandler](
//			context.Background(),
//			ts.CommandBus,
//			ts.RequestReplyBackend,
//			&TestCommand{ID: "1"},
//		)
//
// SendWithReply will send the replies to the channel until the context is canceled or the timeout is exceeded.
//There are multiple cases when more than one reply can be sent:
//   - when the handler returns an error, and the backend is configured to nack the message on error
//     (for the PubSubBackend, it depends on the `PubSubBackendConfig.AckCommandErrors` option),
//   - when you use a fan-out mechanism and commands are handled multiple times by more than one handler).
func SendWithReply[Result any](
	ctx context.Context,
	c CommandBus,
	backend Backend[Result],
	cmd any,
) (replCh <-chan Reply[Result], cancel func(), err error) {
	ctx, cancel = context.WithCancel(ctx)

	defer func() {
		if err != nil {
			cancel()
		}
	}()

	operationID := watermill.NewUUID()

	replyChan, err := backend.ListenForNotifications(ctx, BackendListenForNotificationsParams{
		Command:     cmd,
		OperationID: OperationID(operationID),
	})
	if err != nil {
		return nil, cancel, errors.Wrap(err, "cannot listen for reply")
	}

	if err := c.SendWithModifiedMessage(ctx, cmd, func(m *message.Message) error {
		m.Metadata.Set(OperationIDMetadataKey, operationID)
		return nil
	}); err != nil {
		return nil, cancel, errors.Wrap(err, "cannot send command")
	}

	return replyChan, cancel, nil
}
