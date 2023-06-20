package cqrs

import (
	stdErrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type CommandConfig struct {
	// GeneratePublishTopic is used to generate topic for publishing command.
	// It is required if config is used in CommandBus.
	GeneratePublishTopic GenerateCommandPublishTopicFn

	// GenerateHandlerSubscribeTopic is used to generate topic for subscribing command.
	// It is required if config is used in CommandProcessor.
	GenerateHandlerSubscribeTopic GenerateCommandHandlerSubscribeTopicFn

	// SubscriberConstructor is used to create subscriber for CommandHandler.
	// It is required if config is used in CommandProcessor.
	SubscriberConstructor CommandsSubscriberConstructorWithParams

	// OnSend is called before publishing the command.
	// The *message.Message can be modified.
	//
	// This option is not required.
	OnSend OnCommandSendFn

	// OnHandle is called before handling command.
	// OnHandle works in a similar way to middlewares: you can inject additional logic before and after handling a command.
	//
	// Because of that, you need to explicitly call params.Handler.Handle() to handle the command.
	//   func(params OnCommandHandleParams) (err error) {
	//       // logic before handle
	//		 //  (...)
	//
	//	     err := params.Handler.Handle(params.Message.Context(), params.Command)
	//
	//       // logic after handle
	//		 //  (...)
	//
	//		 return err
	//	 }
	//
	// This option is not required.
	OnHandle OnCommandHandleFn

	// Marshaler is used to marshal and unmarshal commands.
	// It is required.
	Marshaler CommandEventMarshaler

	// Logger instance used to log.
	// If not provided, watermill.NopLogger is used.
	Logger watermill.LoggerAdapter

	// If true, CommandProcessor will ack messages even if CommandHandler returns an error.
	// If RequestReplyEnabled is enabled and sending reply fails, the message will be nack-ed anyway.
	AckCommandHandlingErrors bool
}

func (c *CommandConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c CommandConfig) validateCommon() error {
	var err error

	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	return err
}

func (c CommandConfig) ValidateForProcessor() error {
	var err error

	err = stdErrors.Join(err, c.validateCommon())

	if c.GenerateHandlerSubscribeTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateHandlerSubscribeTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}

func (c CommandConfig) ValidateForBus() error {
	var err error

	err = stdErrors.Join(err, c.validateCommon())

	if c.GeneratePublishTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GeneratePublishTopic"))
	}

	return err
}

type GenerateCommandPublishTopicFn func(GenerateCommandPublishTopicParams) (string, error)

type GenerateCommandPublishTopicParams struct {
	CommandName string
	Command     any
}

type GenerateCommandHandlerSubscribeTopicFn func(GenerateCommandHandlerSubscribeTopicParams) (string, error)

type GenerateCommandHandlerSubscribeTopicParams struct {
	CommandName    string
	CommandHandler CommandHandler
}

type OnCommandSendFn func(params OnCommandSendParams) error

type OnCommandSendParams struct {
	CommandName string
	Command     any

	// Message is never nil and can be modified.
	Message *message.Message
}

type OnCommandHandleFn func(params OnCommandHandleParams) error

type OnCommandHandleParams struct {
	Handler CommandHandler

	CommandName string
	Command     any

	// Message is never nil and can be modified.
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

type CommandsSubscriberConstructorParams struct {
	HandlerName string
	Handler     CommandHandler
}
