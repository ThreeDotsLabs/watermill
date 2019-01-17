package cqrs

import "github.com/ThreeDotsLabs/watermill/message"

type CommandBus struct {
	publisher message.Publisher
	topic     string
	marshaler Marshaler
}

func NewCommandBus(
	publisher message.Publisher,
	topic string,
	marshaler Marshaler,
) CommandBus {
	return CommandBus{publisher, topic, marshaler}
}

func (c CommandBus) Send(cmd interface{}) error {
	msg, err := c.marshaler.Marshal(cmd)
	if err != nil {
		panic(err)
	}

	return c.publisher.Publish(c.topic, msg)
}
