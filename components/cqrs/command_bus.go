package cqrs

import (
	"context"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

// CommandBus transports commands to command handlers.
type CommandBus struct {
	publisher message.Publisher

	config CommandConfig
}

// NewCommandBusWithConfig creates a new CommandBus.
func NewCommandBusWithConfig(publisher message.Publisher, config CommandConfig) (*CommandBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}

	config.setDefaults()
	if err := config.ValidateForBus(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &CommandBus{publisher, config}, nil
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

	return &CommandBus{publisher, CommandConfig{
		GeneratePublishTopic: func(params GenerateCommandPublishTopicParams) (string, error) {
			return generateTopic(params.CommandName), nil
		},
		Marshaler: marshaler,
	}}, nil
}

// Send sends command to the command bus.
func (c CommandBus) Send(ctx context.Context, cmd any) error {
	msg, topicName, err := c.newMessage(ctx, cmd)
	if err != nil {
		return err
	}

	return c.publisher.Publish(topicName, msg)
}

func (c CommandBus) newMessage(ctx context.Context, command any) (*message.Message, string, error) {
	msg, err := c.config.Marshaler.Marshal(command)
	if err != nil {
		return nil, "", err
	}

	commandName := c.config.Marshaler.Name(command)
	topicName, err := c.config.GeneratePublishTopic(GenerateCommandPublishTopicParams{
		CommandName: commandName,
		Command:     &command,
	})
	if err != nil {
		return nil, "", errors.Wrap(err, "cannot generate topic name")
	}

	msg.SetContext(ctx)

	if c.config.OnSend != nil {
		err := c.config.OnSend(OnCommandSendParams{
			CommandName: commandName,
			Command:     command,
			Message:     msg,
		})
		if err != nil {
			return nil, "", errors.Wrap(err, "cannot execute OnSend")
		}
	}

	return msg, topicName, nil
}
