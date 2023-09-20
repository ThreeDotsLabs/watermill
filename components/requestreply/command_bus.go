package requestreply

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type CommandReply[Response any] struct {
	// add Handler prefix? for Err as well?
	HandlerResponse Response

	// HandlerErr contains the error returned by the command handler or by Backend if sending reply failed.
	//
	// If error from handler is returned, CommandHandlerError is returned.
	// If listening for reply timed out, HandlerErr is ReplyTimeoutError.
	// If processing was successful, HandlerErr is nil.
	HandlerErr error

	// todo: should it be present? what if no pubsub backend is used?
	// ReplyMsg contains the reply message from the command handler.
	// Warning: ReplyMsg is nil if timeout occurred.
	ReplyMsg *message.Message
}

// todo: use interface for command bus?

// todo: test
// todo: add cancel func?
// todo: doc that ctx cancelation is super important
// SendAndWait sends command to the command bus and waits for the command execution.
func SendAndWait[Response any](
	ctx context.Context,
	c *cqrs.CommandBus,
	backend Backend[Response],
	cmd any,
) (<-chan CommandReply[Response], error) {
	notificationID := watermill.NewUUID()

	// todo: wait for 1 reply by default?
	replyChan, err := backend.ListenForNotifications(ctx, ListenForNotificationsParams{
		Command:        cmd,
		NotificationID: notificationID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot listen for reply")
	}

	if err := c.SendWithModifiedMessage(ctx, cmd, func(m *message.Message) error {
		m.Metadata.Set(NotificationIdMetadataKey, notificationID)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "cannot send command")
	}

	return replyChan, nil
}
