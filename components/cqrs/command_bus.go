package cqrs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

// CommandBus transports commands to command handlers.
type CommandBus struct {
	publisher message.Publisher

	config CommandProcessorConfig
}

// NewCommandBus creates a new CommandBus.
// Deprecated: use NewCommandBusWithConfig instead.
func NewCommandBus(
	publisher message.Publisher,
	generateTopic func(commandName string) string,
	marshaler CommandEventMarshaler,
) (*CommandBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}
	if generateTopic == nil {
		return nil, errors.New("missing generateTopic")
	}
	if marshaler == nil {
		return nil, errors.New("missing marshaler")
	}

	return &CommandBus{publisher, CommandProcessorConfig{
		GenerateTopic: func(params GenerateCommandsTopicParams) string {
			return generateTopic(params.CommandName)
		},
		Marshaler: marshaler,
		Logger:    watermill.NopLogger{},
	}}, nil
}

// NewCommandBusWithConfig creates a new CommandBus.
func NewCommandBusWithConfig(publisher message.Publisher, config CommandProcessorConfig) (*CommandBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}

	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &CommandBus{publisher, config}, nil
}

// Send sends command to the command bus.
func (c CommandBus) Send(ctx context.Context, cmd any) error {
	msg, topicName, err := c.newMessage(ctx, cmd)
	if err != nil {
		return err
	}

	return c.publisher.Publish(topicName, msg)
}

func (c CommandBus) newMessage(ctx context.Context, cmd any) (*message.Message, string, error) {
	msg, err := c.config.Marshaler.Marshal(cmd)
	if err != nil {
		return nil, "", err
	}

	commandName := c.config.Marshaler.Name(cmd)
	topicName := c.config.GenerateTopic(GenerateCommandsTopicParams{
		CommandName: commandName,
	})

	msg.SetContext(ctx)
	return msg, topicName, nil
}

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

	replyChan, err := c.config.RequestReplyBackend.ListenForReply(ctx, msg, cmd)
	if err != nil {
		return nil, errors.Wrap(err, "cannot listen for reply")
	}

	if err := c.publisher.Publish(topicName, msg); err != nil {
		return nil, err
	}

	return replyChan, nil
}
