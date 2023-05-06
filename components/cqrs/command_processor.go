package cqrs

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

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

// CommandProcessor determines which CommandHandler should handle the command received from the command bus.
type CommandProcessor struct {
	handlers []CommandHandler

	config CommandProcessorConfig
}

type CommandProcessorConfig struct {
	GenerateTopic         GenerateCommandsTopicFn
	SubscriberConstructor CommandsSubscriberConstructorWithParams

	Marshaler CommandEventMarshaler
	Logger    watermill.LoggerAdapter

	// RequestReplyEnabled enables request-reply pattern for commands.
	// Reply is sent **just** from the CommandBus.SendAndWait method.
	// This configuration doesn't affect CommandBus.Send method.
	RequestReplyEnabled bool
	RequestReplyBackend RequestReplyBackend

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

	if c.GenerateTopic == nil {
		err = multierror.Append(err, errors.New("missing GenerateReplyNotificationTopic"))
	}
	if c.SubscriberConstructor == nil {
		err = multierror.Append(err, errors.New("missing SubscriberConstructor"))
	}
	if c.Marshaler == nil {
		err = multierror.Append(err, errors.New("missing Marshaler"))
	}

	if c.RequestReplyEnabled && c.RequestReplyBackend == nil {
		err = multierror.Append(err, errors.New("missing RequestReply.Backend"))
	}

	return err
}

type GenerateCommandsTopicFn func(GenerateCommandsTopicParams) string

type GenerateCommandsTopicParams struct {
	CommandName string
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

	cp, err := NewCommandProcessorWithConfig(CommandProcessorConfig{
		GenerateTopic: func(params GenerateCommandsTopicParams) string {
			return generateTopic(params.CommandName)
		},
		SubscriberConstructor: func(params CommandsSubscriberConstructorParams) (message.Subscriber, error) {
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

func NewCommandProcessorWithConfig(config CommandProcessorConfig) (*CommandProcessor, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &CommandProcessor{
		config: config,
	}, nil
}

func (p *CommandProcessor) AddHandler(handler CommandHandler) {
	p.handlers = append(p.handlers, handler)
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

		topicName := p.config.GenerateTopic(GenerateCommandsTopicParams{
			CommandName: commandName,
		})

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

		subscriber, err := p.config.SubscriberConstructor(CommandsSubscriberConstructorParams{
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

		err := handler.Handle(msg.Context(), cmd)

		var replyErr error
		if p.config.RequestReplyEnabled {
			replyErr = p.config.RequestReplyBackend.OnCommandProcessed(msg, cmd, err)
		}

		if p.config.AckCommandHandlingErrors && err != nil {
			// we want to nack if we are using request-reply,
			// and we failed to send information about failure
			if replyErr != nil {
				return replyErr
			}

			logger.Error("Error when handling command", err, nil)
			return nil
		} else if replyErr != nil {
			err = multierror.Append(err, replyErr)
		}

		if err != nil {
			logger.Debug("Error when handling command", watermill.LogFields{"err": err})
		}

		return err
	}, nil
}

func (p CommandProcessor) validateCommand(cmd interface{}) error {
	// CommandHandler's NewCommand must return a pointer, because it is used to unmarshal
	if err := isPointer(cmd); err != nil {
		return errors.Wrap(err, "command must be a non-nil pointer")
	}

	return nil
}
