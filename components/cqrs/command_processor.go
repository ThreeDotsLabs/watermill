package cqrs

import (
	stdErrors "errors"
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type CommandProcessorConfig struct {
	// GenerateSubscribeTopic is used to generate topic for subscribing command.
	GenerateSubscribeTopic CommandProcessorGenerateSubscribeTopicFn

	// SubscriberConstructor is used to create subscriber for CommandHandler.
	SubscriberConstructor CommandProcessorSubscriberConstructorFn

	// OnHandle is called before handling command.
	// OnHandle works in a similar way to middlewares: you can inject additional logic before and after handling a command.
	//
	// Because of that, you need to explicitly call params.Handler.Handle() to handle the command.
	//   func(params CommandProcessorOnHandleParams) (err error) {
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
	OnHandle CommandProcessorOnHandleFn

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

func (c *CommandProcessorConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c CommandProcessorConfig) Validate() error {
	var err error

	if c.Marshaler == nil {
		err = stdErrors.Join(err, errors.New("missing Marshaler"))
	}

	if c.GenerateSubscribeTopic == nil {
		err = stdErrors.Join(err, errors.New("missing GenerateSubscribeTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = stdErrors.Join(err, errors.New("missing SubscriberConstructor"))
	}

	return err
}

type CommandProcessorGenerateSubscribeTopicFn func(CommandProcessorGenerateSubscribeTopicParams) (string, error)

type CommandProcessorGenerateSubscribeTopicParams struct {
	CommandName    string
	CommandHandler CommandHandler
}

// CommandProcessorSubscriberConstructorFn creates subscriber for CommandHandler.
// It allows you to create a separate customized Subscriber for every command handler.
type CommandProcessorSubscriberConstructorFn func(CommandProcessorSubscriberConstructorParams) (message.Subscriber, error)

type CommandProcessorSubscriberConstructorParams struct {
	HandlerName string
	Handler     CommandHandler
}

type CommandProcessorOnHandleFn func(params CommandProcessorOnHandleParams) error

type CommandProcessorOnHandleParams struct {
	Handler CommandHandler

	CommandName string
	Command     any

	// Message is never nil and can be modified.
	Message *message.Message
}

// CommandProcessor determines which CommandHandler should handle the command received from the command bus.
type CommandProcessor struct {
	handlers []CommandHandler

	config CommandProcessorConfig
}

func NewCommandProcessorWithConfig(config CommandProcessorConfig) (*CommandProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &CommandProcessor{
		config: config,
	}, nil
}

// NewCommandProcessor creates a new CommandProcessor.
// Deprecated. Use NewCommandProcessorWithConfig instead.
func NewCommandProcessor(
	handlers []CommandHandler,
	generateTopic func(commandName string) string,
	subscriberConstructor CommandsSubscriberConstructor,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
) (*CommandProcessor, error) {
	if len(handlers) == 0 {
		return nil, errors.New("missing handlers")
	}
	if generateTopic == nil {
		return nil, errors.New("missing generateTopic")
	}
	if subscriberConstructor == nil {
		return nil, errors.New("missing subscriberConstructor")
	}

	cp, err := NewCommandProcessorWithConfig(CommandProcessorConfig{
		GenerateSubscribeTopic: func(params CommandProcessorGenerateSubscribeTopicParams) (string, error) {
			return generateTopic(params.CommandName), nil
		},
		SubscriberConstructor: func(params CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return subscriberConstructor(params.HandlerName)
		},
		Marshaler: marshaler,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}

	for _, handler := range handlers {
		cp.AddHandler(handler)
	}

	return cp, nil
}

// CommandsSubscriberConstructor creates subscriber for CommandHandler.
// It allows you to create a separate customized Subscriber for every command handler.
//
// Deprecated: please use CommandProcessorSubscriberConstructorFn instead.
type CommandsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// AddHandler adds a new CommandHandler to the CommandProcessor.
//
// It's required to call AddHandlersToRouter to add the handlers to the router after calling AddHandler.
// AddHandlersToRouter should be called only once.
func (p *CommandProcessor) AddHandler(handler ...CommandHandler) {
	p.handlers = append(p.handlers, handler...)
}

// DuplicateCommandHandlerError occurs when a handler with the same name already exists.
type DuplicateCommandHandlerError struct {
	CommandName string
}

func (d DuplicateCommandHandlerError) Error() string {
	return fmt.Sprintf("command handler for command %s already exists", d.CommandName)
}

// AddHandlersToRouter adds the CommandProcessor's handlers to the given router.
// It should be called only once per CommandProcessor instance.
func (p CommandProcessor) AddHandlersToRouter(r *message.Router) error {
	handledCommands := map[string]struct{}{}

	if len(p.Handlers()) == 0 {
		return errors.New("CommandProcessor has no handlers, did you call AddHandlers?")
	}

	for i := range p.Handlers() {
		handler := p.handlers[i]
		handlerName := handler.HandlerName()
		commandName := p.config.Marshaler.Name(handler.NewCommand())

		topicName, err := p.config.GenerateSubscribeTopic(CommandProcessorGenerateSubscribeTopicParams{
			CommandName:    commandName,
			CommandHandler: handler,
		})
		if err != nil {
			return errors.Wrapf(err, "cannot generate topic for command handler %s", handlerName)
		}

		if _, ok := handledCommands[commandName]; ok {
			return DuplicateCommandHandlerError{commandName}
		}
		handledCommands[commandName] = struct{}{}

		logger := p.config.Logger.With(watermill.LogFields{
			"command_handler_name": handlerName,
			"topic":                topicName,
		})

		handlerFunc, err := p.routerHandlerFunc(handler, logger)
		if err != nil {
			return err
		}

		logger.Debug("Adding CQRS command handler to router", nil)

		subscriber, err := p.config.SubscriberConstructor(CommandProcessorSubscriberConstructorParams{
			HandlerName: handlerName,
			Handler:     handler,
		})
		if err != nil {
			return errors.Wrap(err, "cannot create subscriber for command processor")
		}

		r.AddNoPublisherHandler(
			handlerName,
			topicName,
			subscriber,
			handlerFunc,
		)
	}

	return nil
}

// Handlers returns the CommandProcessor's handlers.
func (p CommandProcessor) Handlers() []CommandHandler {
	return p.handlers
}

func (p CommandProcessor) routerHandlerFunc(handler CommandHandler, logger watermill.LoggerAdapter) (message.NoPublishHandlerFunc, error) {
	cmd := handler.NewCommand()
	cmdName := p.config.Marshaler.Name(cmd)

	if err := p.validateCommand(cmd); err != nil {
		return nil, err
	}

	return func(msg *message.Message) error {
		cmd := handler.NewCommand()
		messageCmdName := p.config.Marshaler.NameFromMessage(msg)

		if messageCmdName != cmdName {
			logger.Trace("Received different command type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_command_type": cmdName,
				"received_command_type": messageCmdName,
			})
			return nil
		}

		logger.Debug("Handling command", watermill.LogFields{
			"message_uuid":          msg.UUID,
			"received_command_type": messageCmdName,
		})

		if err := p.config.Marshaler.Unmarshal(msg, cmd); err != nil {
			return err
		}

		handle := func(params CommandProcessorOnHandleParams) (err error) {
			return params.Handler.Handle(params.Message.Context(), params.Command)
		}
		if p.config.OnHandle != nil {
			handle = p.config.OnHandle
		}

		err := handle(CommandProcessorOnHandleParams{
			Handler:     handler,
			CommandName: messageCmdName,
			Command:     cmd,
			Message:     msg,
		})

		if p.config.AckCommandHandlingErrors && err != nil {
			logger.Error("Error when handling command, acking (AckCommandHandlingErrors is enabled)", err, nil)
			return nil
		}
		if err != nil {
			logger.Debug("Error when handling command, nacking", watermill.LogFields{"err": err})
			return err
		}

		return nil
	}, nil
}

func (p CommandProcessor) validateCommand(cmd interface{}) error {
	// CommandHandler's NewCommand must return a pointer, because it is used to unmarshal
	if err := isPointer(cmd); err != nil {
		return errors.Wrap(err, "command must be a non-nil pointer")
	}

	return nil
}
