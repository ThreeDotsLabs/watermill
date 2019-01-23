package amqp

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const MessageUUIDHeaderKey = "_watermill_message_uuid"

type Marshaler interface {
	Marshal(msg *message.Message) (amqp.Publishing, error)
	Unmarshal(amqpMsg amqp.Delivery) (*message.Message, error)
}

type DefaultMarshaler struct {
	PostprocessPublishing func(amqp.Publishing) amqp.Publishing
	NotPersistent         bool
}

func (d DefaultMarshaler) Marshal(msg *message.Message) (amqp.Publishing, error) {
	headers := make(amqp.Table, len(msg.Metadata)+1) // metadata + plus uuid

	for key, value := range msg.Metadata {
		headers[key] = value
	}
	headers[MessageUUIDHeaderKey] = msg.UUID

	publishing := amqp.Publishing{
		Body:    msg.Payload,
		Headers: headers,
	}
	if !d.NotPersistent {
		publishing.DeliveryMode = amqp.Persistent
	}

	if d.PostprocessPublishing != nil {
		publishing = d.PostprocessPublishing(publishing)
	}

	return publishing, nil
}

func (DefaultMarshaler) Unmarshal(amqpMsg amqp.Delivery) (*message.Message, error) {
	msgUUID, ok := amqpMsg.Headers[MessageUUIDHeaderKey]
	if !ok {
		return nil, errors.Errorf("missing %s header", MessageUUIDHeaderKey)
	}

	msgUUIDStr, ok := msgUUID.(string)
	if !ok {
		return nil, errors.Errorf("message UUID is not a string, but: %#v", msgUUID)
	}

	msg := message.NewMessage(msgUUIDStr, amqpMsg.Body)
	msg.Metadata = make(message.Metadata, len(amqpMsg.Headers)-1) // headers - minus uuid

	for key, value := range amqpMsg.Headers {
		if key == MessageUUIDHeaderKey {
			continue
		}

		msg.Metadata[key], ok = value.(string)
		if !ok {
			return nil, errors.Errorf("metadata %s is not a string, but %#v", key, value)
		}
	}

	return msg, nil
}
