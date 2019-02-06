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
	marshaler  Marshaler
	logger     watermill.LoggerAdapter
}

func NewCommandProcessor(
	handlers []CommandHandler,
	commandsTopic string,
	subscriber message.Subscriber,
	marshaler Marshaler,
	logger watermill.LoggerAdapter,
) CommandProcessor {
	return CommandProcessor{
		handlers,
		commandsTopic,
		subscriber,
		marshaler,
		logger,
	}
}

func (p CommandProcessor) Handlers() []CommandHandler {
	return p.handlers
}

// todo - add method which doesnt require router?
func (p CommandProcessor) AddHandlersToRouter(r *message.Router) error {
	for i := range p.Handlers() {
		handler := p.handlers[i]
		commandName := p.marshaler.Name(handler.NewCommand())

		handlerFunc, err := p.RouterHandlerFunc(handler)
		if err != nil {
			return err
		}

		if err := r.AddNoPublisherHandler(
			fmt.Sprintf("command_processor_%s", commandName),
			p.commandsTopic,
			p.subscriber,
			handlerFunc,
		); err != nil {
			return err
		}
	}

	return nil
}

func (p CommandProcessor) RouterHandlerFunc(handler CommandHandler) (message.HandlerFunc, error) {
	cmd := handler.NewCommand()
	cmdName := p.marshaler.Name(cmd)

	if err := p.validateCommand(cmd); err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		cmd := handler.NewCommand()
		messageCmdName := p.marshaler.MarshaledName(msg)

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
	if err := isPointer(cmd); err != nil {
		return errors.Wrap(err, "command must be a not nil pointer")
	}

	return nil
}
