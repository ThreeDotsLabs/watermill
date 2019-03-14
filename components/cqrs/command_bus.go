package cqrs

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

// CommandBus transports commands to command handlers.
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
	if publisher == nil {
		panic("missing publisher")
	}
	if topic == "" {
		panic("missing topic")
	}
	if marshaler == nil {
		panic("missing marshaler")
	}

	return &CommandBus{publisher, topic, marshaler}
}

// Send sends command to the command bus.
func (c CommandBus) Send(ctx context.Context, cmd interface{}) error {
	msg, err := c.marshaler.Marshal(cmd)
	if err != nil {
		return err
	}

	msg.SetContext(ctx)

	return c.publisher.Publish(c.topic, msg)
}
