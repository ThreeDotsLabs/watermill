package cqrs

import (
	stdErrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type CommandConfig struct {
	GenerateBusTopic     GenerateCommandBusTopicFn
	GenerateHandlerTopic GenerateCommandHandlerTopicFn

	SubscriberConstructor CommandsSubscriberConstructorWithParams

	OnSend   OnCommandSendFn
	OnHandle OnCommandHandleFn

	Marshaler CommandEventMarshaler
	Logger    watermill.LoggerAdapter

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

	if c.GenerateBusTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateBusTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}
	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	return err
}

type GenerateCommandBusTopicFn func(GenerateCommandBusTopicParams) (string, error)

type GenerateCommandBusTopicParams struct {
	CommandName string
	Command     any
}

type GenerateCommandHandlerTopicFn func(GenerateCommandHandlerTopicParams) (string, error)

type GenerateCommandHandlerTopicParams struct {
	CommandName    string
	CommandHandler CommandHandler
}

type OnCommandSendFn func(params OnCommandSendParams) error

type OnCommandSendParams struct {
	CommandName string
	Command     any
	Message     *message.Message
}

type OnCommandHandleFn func(params OnCommandHandleParams) error

type OnCommandHandleParams struct {
	Handler CommandHandler
	Command any
	// todo: doc that always present
	Message *message.Message
}
