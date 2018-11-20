package googlecloud

import (
	"cloud.google.com/go/pubsub"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*pubsub.Message, error)
}

type Unmarshaler interface {
	Unmarshal(*pubsub.Message) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

type DefaultMarshaler struct{}

func (m DefaultMarshaler) Marshal(topic string, msg *message.Message) (*pubsub.Message, error) {
	panic("not implemented")
}
