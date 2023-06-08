package cqrs

import (
	stdErrors "errors"
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// CommandProcessor determines which CommandHandler should handle the command received from the command bus.
type CommandProcessor struct {
	handlers []CommandHandler

	config CommandConfig
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

	cp, err := NewCommandProcessorWithConfig(CommandConfig{
		GenerateTopic: func(params GenerateCommandTopicParams) (string, error) {
			return generateTopic(params.CommandName()), nil
		},
		SubscriberConstructor: func(params CommandsSubscriberConstructorParams) (message.Subscriber, error) {
			return subscriberConstructor(params.HandlerName())
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

func NewCommandProcessorWithConfig(config CommandConfig) (*CommandProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &CommandProcessor{
		config: config,
	}, nil
}

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
func (p CommandProcessor) AddHandlersToRouter(r *message.Router) error {
	handledCommands := map[string]struct{}{}

	for i := range p.Handlers() {
		handler := p.handlers[i]
		handlerName := handler.HandlerName()
		commandName := p.config.Marshaler.Name(handler.NewCommand())

		topicName, err := p.config.GenerateTopic(generateCommandHandlerTopicParams{
			commandName:    commandName,
			commandHandler: handler,
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

		subscriber, err := p.config.SubscriberConstructor(commandsSubscriberConstructorParams{
			handlerName: handlerName,
			handler:     handler,
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

		handle := func(params OnCommandHandleParams) error {
			return params.Handler.Handle(params.Message.Context(), params.Command)
		}
		if p.config.OnHandle != nil {
			handle = p.config.OnHandle
		}

		err := handle(OnCommandHandleParams{
			Handler: handler,
			Command: cmd,
			Message: msg,
		})

		var replyErr error
		// todo: test
		if p.config.RequestReplyEnabled {
			replyErr = p.config.RequestReplyBackend.OnCommandProcessed(msg, cmd, err)
		}


		if p.config.AckCommandHandlingErrors && err != nil {
			// we want to nack if we are using request-reply,
			// and we failed to send information about failure
			// todo: test
			if replyErr != nil {
				return replyErr
			}

			logger.Error("Error when handling command", err, nil)
			return nil
		} else if replyErr != nil {
			// todo: test
			err = stdErrors.Join(err, replyErr)
		}

		if err != nil {
			logger.Debug("Error when handling command", watermill.LogFields{"err": err})
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
