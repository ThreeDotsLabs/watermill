package amqp

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const MessageUUIDHeaderKey = "_watermill_message_uuid"

// Marshaler marshals Watermill's message to amqp.Publishing and unmarshals amqp.Delivery to Watermill's message.
type Marshaler interface {
	Marshal(msg *message.Message) (amqp.Publishing, error)
	Unmarshal(amqpMsg amqp.Delivery) (*message.Message, error)
}

type DefaultMarshaler struct {
	// PostprocessPublishing can be used to make some extra processing with amqp.Publishing,
	// for example add CorrelationId and ContentType:
	//
	//  amqp.DefaultMarshaler{
	//		PostprocessPublishing: func(publishing stdAmqp.Publishing) stdAmqp.Publishing {
	//			publishing.CorrelationId = "correlation"
	//			publishing.ContentType = "application/json"
	//
	//			return publishing
	//		},
	//	}
	PostprocessPublishing func(amqp.Publishing) amqp.Publishing

	// When true, DeliveryMode will be not set to Persistent.
	//
	// DeliveryMode Transient means higher throughput, but messages will not be
	// restored on broker restart. The delivery mode of publishings is unrelated
	// to the durability of the queues they reside on. Transient messages will
	// not be restored to durable queues, persistent messages will be restored to
	// durable queues and lost on non-durable queues during server restart.
	NotPersistentDeliveryMode bool
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
	if !d.NotPersistentDeliveryMode {
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
