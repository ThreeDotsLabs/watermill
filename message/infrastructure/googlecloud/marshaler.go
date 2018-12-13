package googlecloud

import (
	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Marshaler transforms a Waterfall Message into the Google Cloud client library Message.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*pubsub.Message, error)
}

// Unmarshaler transforms a Google Cloud client library Message into the Waterfall Message.
type Unmarshaler interface {
	Unmarshal(*pubsub.Message) (*message.Message, error)
}

// UUIDHeaderKey is the key of the Pub/Sub attribute that carries Waterfall UUID.
const UUIDHeaderKey = "_watermill_message_uuid"

// DefaultMarshalerUnmarshaler implements Marshaler and Unmarshaler in the following way:
// All Google Cloud Pub/Sub attributes are equivalent to Waterfall Message metadata.
// Waterfall Message UUID is equivalent to an attribute with `UUIDHeaderKey` as key.
type DefaultMarshalerUnmarshaler struct{}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

func (m DefaultMarshalerUnmarshaler) Marshal(topic string, msg *message.Message) (*pubsub.Message, error) {
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
		Data:       msg.Payload,
		Attributes: attributes,
	}

	return marshaledMsg, nil
}

func (u DefaultMarshalerUnmarshaler) Unmarshal(pubsubMsg *pubsub.Message) (*message.Message, error) {
	metadata := make(message.Metadata, len(pubsubMsg.Attributes))

	var id string
	for k, attr := range pubsubMsg.Attributes {
		if k == UUIDHeaderKey {
			id = attr
			continue
		}
		metadata.Set(k, attr)
	}

	metadata.Set("publishTime", pubsubMsg.PublishTime.String())

	msg := message.NewMessage(id, pubsubMsg.Data)
	msg.Metadata = metadata

	return msg, nil
}
