package cqrs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type CommandReply struct {
	// HandlerErr contains the error returned by the command handler or by RequestReplyBackend if sending reply failed.
	//
	// If error from handler is returned, CommandHandlerError is returned.
	// If listening for reply timed out, HandlerErr is ReplyTimeoutError.
	// If processing was successful, HandlerErr is nil.
	Err error

	// ReplyMsg contains the reply message from the command handler.
	// Warning: ReplyMsg is nil if timeout occurred.
	ReplyMsg *message.Message
}

// todo: test
// todo: add cancel func?
// SendAndWait sends command to the command bus and waits for the command execution.
func (c CommandBus) SendAndWait(ctx context.Context, cmd interface{}) (<-chan CommandReply, error) {
	if !c.config.RequestReplyEnabled {
		return nil, errors.New("RequestReply is not enabled in config")
	}

	msg, topicName, err := c.newMessage(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if err := c.config.RequestReplyBackend.ModifyCommandMessageBeforePublish(msg, cmd); err != nil {
		return nil, errors.Wrap(err, "cannot modify command message before publish")
	}

	// todo: wait for 1 reply by default?
	replyChan, err := c.config.RequestReplyBackend.ListenForReply(ctx, msg, cmd)
	if err != nil {
		return nil, errors.Wrap(err, "cannot listen for reply")
	}

	if err := c.publisher.Publish(topicName, msg); err != nil {
		return nil, err
	}

	return replyChan, nil
}
