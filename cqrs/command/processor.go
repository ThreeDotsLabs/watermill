package command

import (
	"fmt"
	"reflect"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Processor struct {
	handlers      []Handler
	commandsTopic string

	subscriber message.Subscriber
	marshaler  Marshaler
	logger     watermill.LoggerAdapter
}

func NewProcessor(
	handlers []Handler,
	commandsTopic string,
	subscriber message.Subscriber,
	marshaler Marshaler,
	logger watermill.LoggerAdapter,
) Processor {
	return Processor{
		handlers,
		commandsTopic,
		subscriber,
		marshaler,
		logger,
	}
}

func (p Processor) AddHandlersToRouter(r *message.Router) error {
	for i := range p.handlers {
		handler := p.handlers[i]
		commandName := p.marshaler.CommandName(handler.NewCommand())

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

func (p Processor) RouterHandlerFunc(handler Handler) (message.HandlerFunc, error) {
	initCommand := handler.NewCommand()
	expectedCmdName := p.marshaler.CommandName(initCommand)

	if err := p.validateCommand(initCommand); err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		cmd := handler.NewCommand()
		messageCmdName := p.marshaler.MarshaledCommandName(msg)

		if messageCmdName != expectedCmdName {
			p.logger.Trace("Received different command type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_command_type": expectedCmdName,
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

func (p Processor) validateCommand(cmd interface{}) error {
	rv := reflect.ValueOf(cmd)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return NonPointerCommandError{rv.Type()}
	}

	return nil
}

type NonPointerCommandError struct {
	Type reflect.Type
}

func (e NonPointerCommandError) Error() string {
	return "non-pointer command: " + e.Type.String() + ", handler.NewCommand() should return pointer to the command"
}
