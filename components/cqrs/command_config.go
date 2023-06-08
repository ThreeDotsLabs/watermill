package cqrs

import (
	stdErrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type CommandConfig struct {
	GenerateTopic GenerateCommandTopicFn

	SubscriberConstructor CommandsSubscriberConstructorWithParams

	OnSend   OnCommandSendFn
	OnHandle OnCommandHandleFn

	// RequestReplyEnabled enables request-reply pattern for commands.
	// Reply is sent **just** from the CommandBus.SendAndWait method.
	// This configuration doesn't affect CommandBus.Send method.
	RequestReplyEnabled bool
	RequestReplyBackend RequestReplyBackend

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

	if c.GenerateTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}
	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	if c.RequestReplyEnabled && c.RequestReplyBackend == nil {
		err = stdErrors.Join(err, errors.New("missing RequestReply.Backend"))
	}

	return err
}

type GenerateCommandTopicFn func(GenerateCommandTopicParams) (string, error)

type GenerateCommandTopicParams interface {
	CommandName() string
}

type GenerateCommandBusTopicParams interface {
	GenerateCommandTopicParams
	Command() any
}

type generateCommandBusTopicParams struct {
	commandName string
	command     any
}

func (g generateCommandBusTopicParams) CommandName() string {
	return g.commandName
}

func (g generateCommandBusTopicParams) Command() any {
	return g.command
}

type GenerateCommandHandlerTopicParams interface {
	GenerateCommandTopicParams
	CommandHandler() CommandHandler
}

type generateCommandHandlerTopicParams struct {
	commandName    string
	commandHandler CommandHandler
}

func (g generateCommandHandlerTopicParams) CommandName() string {
	return g.commandName
}

func (g generateCommandHandlerTopicParams) CommandHandler() CommandHandler {
	return g.commandHandler
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

// CommandsSubscriberConstructor creates subscriber for CommandHandler.
// It allows you to create a separate customized Subscriber for every command handler.
//
// Deprecated: please use CommandsSubscriberConstructorWithParams instead.
type CommandsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// CommandsSubscriberConstructorWithParams creates subscriber for CommandHandler.
// It allows you to create a separate customized Subscriber for every command handler.
type CommandsSubscriberConstructorWithParams func(CommandsSubscriberConstructorParams) (message.Subscriber, error)

type CommandsSubscriberConstructorParams interface {
	HandlerName() string
	Handler() CommandHandler
}

type commandsSubscriberConstructorParams struct {
	handlerName string
	handler     CommandHandler
}

func (c commandsSubscriberConstructorParams) HandlerName() string {
	return c.handlerName
}

func (c commandsSubscriberConstructorParams) Handler() CommandHandler {
	return c.handler
}
