package cqrs

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// CommandHandler receives a command defined by NewCommand and handles it with the Handle method.
// If using DDD, CommandHandler may modify and persist the aggregate.
//
// In contrast to EvenHandler, every Command must have only one CommandHandler.
type CommandHandler interface {
	NewCommand() interface{}
	Handle(cmd interface{}) error
}

type CommandsSubscriberConstructor func(handlerName string) (message.Subscriber, error)

// CommandProcessor determines which CommandHandler should handle the command received from the command bus.
type CommandProcessor struct {
	handlers      []CommandHandler
	commandsTopic string

	subscriberConstructor CommandsSubscriberConstructor

	marshaler CommandEventMarshaler
	logger    watermill.LoggerAdapter
}

func NewCommandProcessor(
	handlers []CommandHandler,
	commandsTopic string,
	subscriberConstructor CommandsSubscriberConstructor,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
) *CommandProcessor {
	if len(handlers) == 0 {
		panic("missing handlers")
	}
	if commandsTopic == "" {
		panic("empty commandsTopic name")
	}
	if subscriberConstructor == nil {
		panic("missing subscriberConstructor")
	}
	if marshaler == nil {
		panic("missing marshaler")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &CommandProcessor{
		handlers,
		commandsTopic,
		subscriberConstructor,
		marshaler,
		logger,
	}
}

func (p CommandProcessor) AddHandlersToRouter(r *message.Router) error {
	for i := range p.Handlers() {
		handler := p.handlers[i]

		commandName := p.marshaler.Name(handler.NewCommand())
		handlerName := fmt.Sprintf("command_processor-%s", commandName) // todo - should be passed implicit!

		logger := p.logger.With(watermill.LogFields{"handler_name": handlerName})

		handlerFunc, err := p.RouterHandlerFunc(handler, logger)
		if err != nil {
			return err
		}

		logger.Debug("Adding CQRS handler to router", watermill.LogFields{
			"handler_name": handlerName,
		})

		subscriber, err := p.subscriberConstructor(handlerName)
		if err != nil {
			return errors.Wrap(err, "cannot create subscriber for command processor")
		}

		r.AddNoPublisherHandler(
			handlerName,
			p.commandsTopic,
			subscriber,
			handlerFunc,
		)
	}

	return nil
}

func (p CommandProcessor) Handlers() []CommandHandler {
	return p.handlers
}

func (p CommandProcessor) RouterHandlerFunc(handler CommandHandler, logger watermill.LoggerAdapter) (message.HandlerFunc, error) {
	cmd := handler.NewCommand()
	cmdName := p.marshaler.Name(cmd)

	if err := p.validateCommand(cmd); err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		cmd := handler.NewCommand()
		messageCmdName := p.marshaler.NameFromMessage(msg)

		if messageCmdName != cmdName {
			logger.Trace("Received different command type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_command_type": cmdName,
				"received_command_type": messageCmdName,
			})
			return nil, nil
		}

		logger.Debug("Handling command", watermill.LogFields{
			"message_uuid":          msg.UUID,
			"received_command_type": messageCmdName,
		})

		if err := p.marshaler.Unmarshal(msg, cmd); err != nil {
			return nil, err
		}

		if err := handler.Handle(cmd); err != nil {
			logger.Debug("Error when handling command", watermill.LogFields{"err": err})
			return nil, err
		}

		return nil, nil
	}, nil
}

func (p CommandProcessor) validateCommand(cmd interface{}) error {
	// CommandHandler's NewCommand must return a pointer, because it is used to unmarshal
	if err := isPointer(cmd); err != nil {
		return errors.Wrap(err, "command must be a non-nil pointer")
	}

	return nil
}
