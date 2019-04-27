package cqrs

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// CommandBus transports commands to command handlers.
type CommandBus struct {
	publisher     message.Publisher
	generateTopic CommandTopicGenerator
	marshaler     CommandEventMarshaler
}

func NewCommandBus(
	publisher message.Publisher,
	generateTopic CommandTopicGenerator,
	marshaler CommandEventMarshaler,
) *CommandBus {
	if publisher == nil {
		panic("missing publisher")
	}
	if generateTopic == nil {
		panic("missing generateTopic")
	}
	if marshaler == nil {
		panic("missing marshaler")
	}

	return &CommandBus{publisher, generateTopic, marshaler}
}

// Send sends command to the command bus.
func (c CommandBus) Send(cmd interface{}) error {
	msg, err := c.marshaler.Marshal(cmd)
	if err != nil {
		return err
	}

	commandName := c.marshaler.Name(cmd)
	topicName := c.generateTopic(commandName)

	return c.publisher.Publish(topicName, msg)
}
