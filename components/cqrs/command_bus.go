package cqrs

import (
	"context"
	stdErrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type CommandBusConfig struct {
	// GeneratePublishTopic is used to generate topic for publishing command.
	GeneratePublishTopic CommandBusGeneratePublishTopicFn

	// OnSend is called before publishing the command.
	// The *message.Message can be modified.
	//
	// This option is not required.
	OnSend CommandBusOnSendFn

	// Marshaler is used to marshal and unmarshal commands.
	// It is required.
	Marshaler CommandEventMarshaler

	// Logger instance used to log.
	// If not provided, watermill.NopLogger is used.
	Logger watermill.LoggerAdapter
}

func (c *CommandBusConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c CommandBusConfig) Validate() error {
	var err error

	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	if c.GeneratePublishTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GeneratePublishTopic"))
	}

	return err
}

type CommandBusGeneratePublishTopicFn func(CommandBusGeneratePublishTopicParams) (string, error)

type CommandBusGeneratePublishTopicParams struct {
	CommandName string
	Command     any
}

type CommandBusOnSendFn func(params CommandBusOnSendParams) error

type CommandBusOnSendParams struct {
	CommandName string
	Command     any

	// Message is never nil and can be modified.
	Message *message.Message
}

// CommandBus transports commands to command handlers.
type CommandBus struct {
	publisher message.Publisher

	config CommandBusConfig
}

// NewCommandBusWithConfig creates a new CommandBus.
func NewCommandBusWithConfig(publisher message.Publisher, config CommandBusConfig) (*CommandBus, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}

	config.setDefaults()
	if err := config.Validate(); err != nil {
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

	return &CommandBus{publisher, CommandBusConfig{
		GeneratePublishTopic: func(params CommandBusGeneratePublishTopicParams) (string, error) {
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
	topicName, err := c.config.GeneratePublishTopic(CommandBusGeneratePublishTopicParams{
		CommandName: commandName,
		Command:     command,
	})
	if err != nil {
		return nil, "", errors.Wrap(err, "cannot generate topic name")
	}

	msg.SetContext(ctx)

	if c.config.OnSend != nil {
		err := c.config.OnSend(CommandBusOnSendParams{
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
