package kafka

import (
	"github.com/ThreeDotsLabs/watermill/message"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*confluentKafka.Message, error)
}

type Unmarshaler interface {
	Unmarshal(*confluentKafka.Message) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

const UUIDHeaderKey = "_watermill_message_uuid"

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*confluentKafka.Message, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermil for message UUID", UUIDHeaderKey)
	}

	headers := []confluentKafka.Header{{
		Key:   UUIDHeaderKey,
		Value: []byte(msg.UUID),
	}}
	for key, value := range msg.Metadata {
		headers = append(headers, confluentKafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	return &confluentKafka.Message{
		TopicPartition: confluentKafka.TopicPartition{Topic: &topic, Partition: confluentKafka.PartitionAny},
		Value:          msg.Payload,
		Headers:        headers,
	}, nil
}

func (DefaultMarshaler) Unmarshal(kafkaMsg *confluentKafka.Message) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(kafkaMsg.Headers))

	for _, header := range kafkaMsg.Headers {
		if header.Key == UUIDHeaderKey {
			messageID = string(header.Value)
		} else {
			metadata.Set(header.Key, string(header.Value))
		}
	}

	msg := message.NewMessage(
		messageID,
		kafkaMsg.Value,
	)
	msg.Metadata = metadata

	return msg, nil
}

type GeneratePartitionKey func(topic string, msg *message.Message) (string, error)

type kafkaJsonWithPartitioning struct {
	DefaultMarshaler

	generatePartitionKey GeneratePartitionKey
}

func NewWithPartitioningMarshaler(generatePartitionKey GeneratePartitionKey) MarshalerUnmarshaler {
	return kafkaJsonWithPartitioning{generatePartitionKey: generatePartitionKey}
}

func (j kafkaJsonWithPartitioning) Marshal(topic string, msg *message.Message) (*confluentKafka.Message, error) {
	kafkaMsg, err := j.DefaultMarshaler.Marshal(topic, msg)
	if err != nil {
		return nil, err
	}

	key, err := j.generatePartitionKey(topic, msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate partition key")
	}
	kafkaMsg.Key = []byte(key)

	return kafkaMsg, nil
}
