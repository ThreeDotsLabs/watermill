package googlecloud

import (
	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*pubsub.Message, error)
}

type Unmarshaler interface {
	Unmarshal(*pubsub.Message) (*message.Message, error)
}

const UUIDHeaderKey = "_watermill_message_uuid"

type DefaultMarshaler struct{}
type DefaultUnmarshaler struct{}

func (m DefaultMarshaler) Marshal(topic string, msg *message.Message) (*pubsub.Message, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	attributes := map[string]string{
		UUIDHeaderKey: msg.UUID,
	}

	for k, v := range msg.Metadata {
		attributes[k] = v
	}

	marshaledMsg := &pubsub.Message{
		Data:       []byte(msg.Payload),
		Attributes: attributes,
	}

	return marshaledMsg, nil
}

func (u DefaultUnmarshaler) Unmarshal(*pubsub.Message) (*message.Message, error) {
	panic("implement me")
}
