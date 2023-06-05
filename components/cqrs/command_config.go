package cqrs

import (
	stdErrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
)

type CommandConfig struct {
	GenerateTopic         GenerateCommandsTopicFn
	SubscriberConstructor CommandsSubscriberConstructorWithParams

	Marshaler CommandEventMarshaler
	Logger    watermill.LoggerAdapter

	// todo: better naming?
	// If true, CommandProcessor will ack messages even if CommandHandler returns an error.
	// If RequestReplyEnabled is enabled and sending reply fails, the message will be nack-ed anyway.
	AckCommandHandlingErrors bool
}

func (c *CommandConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c CommandConfig) Validate() error {
	var err error

	if c.GenerateTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}
	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	return err
}

type GenerateCommandsTopicFn func(GenerateCommandsTopicParams) string

type GenerateCommandsTopicParams struct {
	CommandName string
}
