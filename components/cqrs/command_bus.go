package cqrs

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

type CommandBus struct {
	publisher message.Publisher
	topic     string
	marshaler CommandEventMarshaler
}

func NewCommandBus(
	publisher message.Publisher,
	topic string,
	marshaler CommandEventMarshaler,
) *CommandBus {
	return &CommandBus{publisher, topic, marshaler}
}

func (c CommandBus) Send(cmd interface{}) error {
	msg, err := c.marshaler.Marshal(cmd)
	if err != nil {
		return err
	}

	return c.publisher.Publish(c.topic, msg)
}
