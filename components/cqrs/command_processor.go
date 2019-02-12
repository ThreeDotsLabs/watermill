package cqrs

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type CommandHandler interface {
	NewCommand() interface{}
	Handle(cmd interface{}) error
}

type CommandProcessor struct {
	handlers      []CommandHandler
	commandsTopic string

	subscriber message.Subscriber
	marshaler  CommandEventMarshaler
	logger     watermill.LoggerAdapter
}

func NewCommandProcessor(
	handlers []CommandHandler,
	commandsTopic string,
	subscriber message.Subscriber,
	marshaler CommandEventMarshaler,
	logger watermill.LoggerAdapter,
) *CommandProcessor {
	if len(handlers) == 0 {
		panic("missing handlers")
	}
	if commandsTopic == "" {
		panic("empty commandsTopic name")
	}
	if subscriber == nil {
		panic("missing subscriber")
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
		subscriber,
		marshaler,
		logger,
	}
}

func (p CommandProcessor) AddHandlersToRouter(r *message.Router) error {
	for i := range p.Handlers() {
		handler := p.handlers[i]
		commandName := p.marshaler.Name(handler.NewCommand())

		handlerFunc, err := p.RouterHandlerFunc(handler)
		if err != nil {
			return err
		}

		handlerName := fmt.Sprintf("command_processor-%s", commandName)
		p.logger.Debug("Adding CQRS handler to router", watermill.LogFields{
			"handler_name": handlerName,
		})

		r.AddNoPublisherHandler(
			handlerName,
			p.commandsTopic,
			p.subscriber,
			handlerFunc,
		)
	}

	return nil
}

func (p CommandProcessor) Handlers() []CommandHandler {
	return p.handlers
}

func (p CommandProcessor) RouterHandlerFunc(handler CommandHandler) (message.HandlerFunc, error) {
	cmd := handler.NewCommand()
	cmdName := p.marshaler.Name(cmd)

	if err := p.validateCommand(cmd); err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		cmd := handler.NewCommand()
		messageCmdName := p.marshaler.NameFromMessage(msg)

		if messageCmdName != cmdName {
			p.logger.Trace("Received different command type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_command_type": cmdName,
				"received_command_type": messageCmdName,
			})
			return nil, nil
		}

		p.logger.Debug("Handling command", watermill.LogFields{
			"message_uuid":          msg.UUID,
			"received_command_type": messageCmdName,
		})

		if err := p.marshaler.Unmarshal(msg, cmd); err != nil {
			return nil, err
		}

		if err := handler.Handle(cmd); err != nil {
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
