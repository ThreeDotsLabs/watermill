package command

import "github.com/ThreeDotsLabs/watermill/message"

type Bus struct {
	publisher message.Publisher
	topic     string
	marshaler Marshaler
}

func NewBus(
	publisher message.Publisher,
	topic string,
	marshaler Marshaler,
) Bus {
	return Bus{publisher, topic, marshaler}
}

func (c Bus) Send(cmd interface{}) error {
	msg, err := c.marshaler.Marshal(cmd)
	if err != nil {
		panic(err)
	}

	return c.publisher.Publish(c.topic, msg)
}
