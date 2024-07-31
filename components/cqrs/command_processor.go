package cqrs

import (
	"errors"
	"fmt"

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
	//  func(params CommandProcessorOnHandleParams) (err error) {
	//      // logic before handle
	//      //  (...)
	//
	//      err := params.Handler.Handle(params.Message.Context(), params.Command)
	//
	//      // logic after handle
	//      //  (...)
	//
	//      return err
	//  }
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
	// If RequestReplyBackend is not null and sending reply fails, the message will be nack-ed anyway.
	//
	// Warning: It's not recommended to use this option when you are using requestreply component
	// (requestreply.NewCommandHandler or requestreply.NewCommandHandlerWithResult), as it may ack the
	// command when sending reply failed.
	//
	// When you are using requestreply, you should use requestreply.PubSubBackendConfig.AckCommandErrors.
	AckCommandHandlingErrors bool

	// disableRouterAutoAddHandlers is used to keep backwards compatibility.
	// it is set when CommandProcessor is created by NewCommandProcessor.
	// Deprecated: please migrate to NewCommandProcessorWithConfig.
	disableRouterAutoAddHandlers bool
}

func (c *CommandProcessorConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c CommandProcessorConfig) Validate() error {
	var err error

	if c.Marshaler == nil {
		err = errors.Join(err, errors.New("missing Marshaler"))
	}

	if c.GenerateSubscribeTopic == nil {
		err = errors.Join(err, errors.New("missing GenerateSubscribeTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = errors.Join(err, errors.New("missing SubscriberConstructor"))
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
	router *message.Router

	handlers []CommandHandler

	config CommandProcessorConfig
}

func NewCommandProcessorWithConfig(router *message.Router, config CommandProcessorConfig) (*CommandProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if router == nil && !config.disableRouterAutoAddHandlers {
		return nil, errors.New("missing router")
	}

	return &CommandProcessor{
		router: router,
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

	cp, err := NewCommandProcessorWithConfig(
		nil,
		CommandProcessorConfig{
			GenerateSubscribeTopic: func(params CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return generateTopic(params.CommandName), nil
			},
			SubscriberConstructor: func(params CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return subscriberConstructor(params.HandlerName)
			},
			Marshaler:                    marshaler,
			Logger:                       logger,
			disableRouterAutoAddHandlers: true,
		},
	)
	if err != nil {
		return nil, err
	}

	for _, handler := range handlers {
		if err := cp.AddHandlers(handler); err != nil {
			return nil, err
		}
	}

	return cp, nil
}

// CommandsSubscriberConstructor creates subscriber for CommandHandler.
// It allows you to create a separate customized Subscriber for every command handler.
//
// Deprecated: please use CommandProcessorSubscriberConstructorFn instead.
type CommandsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// AddHandlers adds a new CommandHandler to the CommandProcessor and adds it to the router.
func (p *CommandProcessor) AddHandlers(handlers ...CommandHandler) error {
	handledCommands := map[string]struct{}{}
	for _, handler := range handlers {
		commandName := p.config.Marshaler.Name(handler.NewCommand())
		if _, ok := handledCommands[commandName]; ok {
			return DuplicateCommandHandlerError{commandName}
		}

		handledCommands[commandName] = struct{}{}
	}

	if p.config.disableRouterAutoAddHandlers {
		p.handlers = append(p.handlers, handlers...)
		return nil
	}

	for _, handler := range handlers {
		if err := p.addHandlerToRouter(p.router, handler); err != nil {
			return err
		}

		p.handlers = append(p.handlers, handler)
	}

	return nil
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
//
// It is required to call AddHandlersToRouter only if command processor is created with NewCommandProcessor (disableRouterAutoAddHandlers is set to true).
// Deprecated: please migrate to command processor created by NewCommandProcessorWithConfig.
func (p CommandProcessor) AddHandlersToRouter(r *message.Router) error {
	if !p.config.disableRouterAutoAddHandlers {
		return errors.New("AddHandlersToRouter should be called only when using deprecated NewCommandProcessor")
	}

	for i := range p.Handlers() {
		handler := p.handlers[i]

		if err := p.addHandlerToRouter(r, handler); err != nil {
			return err
		}
	}

	return nil
}

func (p CommandProcessor) addHandlerToRouter(r *message.Router, handler CommandHandler) error {
	handlerName := handler.HandlerName()
	commandName := p.config.Marshaler.Name(handler.NewCommand())

	topicName, err := p.config.GenerateSubscribeTopic(CommandProcessorGenerateSubscribeTopicParams{
		CommandName:    commandName,
		CommandHandler: handler,
	})
	if err != nil {
		return fmt.Errorf("cannot generate topic for command handler %s: %w", handlerName, err)
	}

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
		return fmt.Errorf("cannot create subscriber for command processor: %w", err)
	}

	r.AddNoPublisherHandler(
		handlerName,
		topicName,
		subscriber,
		handlerFunc,
	)

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

		ctx := CtxWithOriginalMessage(msg.Context(), msg)
		msg.SetContext(ctx)

		if err := p.config.Marshaler.Unmarshal(msg, cmd); err != nil {
			return err
		}

		handle := func(params CommandProcessorOnHandleParams) (err error) {
			return params.Handler.Handle(ctx, params.Command)
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
		return fmt.Errorf("command must be a non-nil pointer: %w", err)
	}

	return nil
}
