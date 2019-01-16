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

func (c Processor) AddHandlersToRouter(r *message.Router) error {
	for i := range c.handlers {
		handler := c.handlers[i]
		commandName := c.marshaler.CommandName(handler.NewCommand())

		handlerFunc, err := c.RouterHandlerFunc(handler)
		if err != nil {
			return err
		}

		if err := r.AddNoPublisherHandler(
			fmt.Sprintf("command_processor_%s", commandName),
			c.commandsTopic,
			c.subscriber,
			handlerFunc,
		); err != nil {
			return err
		}
	}

	return nil
}

func (c Processor) RouterHandlerFunc(handler Handler) (message.HandlerFunc, error) {
	initCommand := handler.NewCommand()
	expectedCmdName := c.marshaler.CommandName(initCommand)

	rv := reflect.ValueOf(initCommand)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return nil, NonPointerCommandError{rv.Type()}
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		cmd := handler.NewCommand()
		messageCmdName := c.marshaler.MarshaledCommandName(msg)

		if messageCmdName != expectedCmdName {
			c.logger.Trace("Received different command type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_command_type": expectedCmdName,
				"received_command_type": messageCmdName,
			})
			return nil, nil
		}

		c.logger.Debug("Handling command", watermill.LogFields{
			"message_uuid":          msg.UUID,
			"received_command_type": messageCmdName,
		})

		if err := c.marshaler.Unmarshal(msg, cmd); err != nil {
			return nil, err
		}

		if err := handler.Handle(cmd); err != nil {
			return nil, err
		}

		return nil, nil
	}, nil
}

type NonPointerCommandError struct {
	Type reflect.Type
}

func (e NonPointerCommandError) Error() string {
	return "non-pointer command: " + e.Type.String() + ", handler.NewCommand() should return pointer to the command"
}
