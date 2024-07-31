package requestreply

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type CommandBus interface {
	SendWithModifiedMessage(ctx context.Context, cmd any, modify func(*message.Message) error) error
}

// SendWithReply sends command to the command bus and receives a replies of the command handler.
// It returns a channel with replies, cancel function and error.
// If more than one replies are sent, only the first which is received is returned.
//
// If you expect multiple replies, please use SendWithReplies instead.
//
// SendWithReply is blocking until the first reply is received or the context is canceled.
// SendWithReply can be cancelled by cancelling context or
// by exceeding the timeout set in the backend (if set).
//
// SendWithReply can listen for handlers with results (NewCommandHandlerWithResult) and without results (NewCommandHandler).
// If you are listening for handlers without results, you should pass `NoResult` or `struct{}` as `Result` generic type:
//
//	 reply, err := requestreply.SendWithReply[requestreply.NoResult](
//			context.Background(),
//			ts.CommandBus,
//			ts.RequestReplyBackend,
//			&TestCommand{ID: "1"},
//		)
//
// If `NewCommandHandlerWithResult` handler returns a specific type, you should pass it as `Result` generic type:
//
//	 reply, err := requestreply.SendWithReply[SomeTypeReturnedByHandler](
//			context.Background(),
//			ts.CommandBus,
//			ts.RequestReplyBackend,
//			&TestCommand{ID: "1"},
//		)
func SendWithReply[Result any](
	ctx context.Context,
	c CommandBus,
	backend Backend[Result],
	cmd any,
) (Reply[Result], error) {
	replyCh, cancel, err := SendWithReplies[Result](ctx, c, backend, cmd)
	if err != nil {
		return Reply[Result]{}, fmt.Errorf("SendWithReplies failed: %w", err)
	}
	defer cancel()

	select {
	case <-ctx.Done():
		return Reply[Result]{}, fmt.Errorf("context closed: %w", ctx.Err())
	case reply := <-replyCh:
		return reply, nil
	}
}

// SendWithReplies sends command to the command bus and receives a replies of the command handler.
// It returns a channel with replies, cancel function and error.
//
// SendWithReplies can be cancelled by calling cancel function or by cancelling context or
// When SendWithReplies is canceled, the returned channel is closed as well.
// by exceeding the timeout set in the backend (if set).
// Warning: It's important to cancel the function, because it's listening for the replies in the background.
// Lack of cancelling the function can lead to subscriber leak.
//
// SendWithReplies can listen for handlers with results (NewCommandHandlerWithResult) and without results (NewCommandHandler).
// If you are listening for handlers without results, you should pass `NoResult` or `struct{}` as `Result` generic type:
//
//	 replyCh, cancel, err := requestreply.SendWithReplies[requestreply.NoResult](
//			context.Background(),
//			ts.CommandBus,
//			ts.RequestReplyBackend,
//			&TestCommand{ID: "1"},
//		)
//
// If `NewCommandHandlerWithResult` handler returns a specific type, you should pass it as `Result` generic type:
//
//	 replyCh, cancel, err := requestreply.SendWithReplies[SomeTypeReturnedByHandler](
//			context.Background(),
//			ts.CommandBus,
//			ts.RequestReplyBackend,
//			&TestCommand{ID: "1"},
//		)
//
// SendWithReplies will send the replies to the channel until the context is cancelled or the timeout is exceeded.
// They are multiple cases when more than one reply can be sent:
//   - when the handler returns an error, and backend is configured to nack the message on error
//     (for the PubSubBackend, it depends on `PubSubBackendConfig.AckCommandErrors` option.),
//   - when you are using fan-out mechanism and commands are handled multiple times,
func SendWithReplies[Result any](
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
		return nil, cancel, fmt.Errorf("cannot listen for reply: %w", err)
	}

	if err := c.SendWithModifiedMessage(ctx, cmd, func(m *message.Message) error {
		m.Metadata.Set(OperationIDMetadataKey, operationID)
		return nil
	}); err != nil {
		return nil, cancel, fmt.Errorf("cannot send command: %w", err)
	}

	return replyChan, cancel, nil
}
