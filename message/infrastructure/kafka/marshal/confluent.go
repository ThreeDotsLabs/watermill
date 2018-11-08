package marshal

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// todo - rename
type ConfluentKafka struct{}

func (ConfluentKafka) Marshal(topic string, msg *message.Message) (*confluentKafka.Message, error) {
	headers := []confluentKafka.Header{{
		Key:   "uuid", // todo - make it reserved
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

func (ConfluentKafka) Unmarshal(kafkaMsg *confluentKafka.Message) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(kafkaMsg.Headers))

	for _, header := range kafkaMsg.Headers {
		if header.Key == "uuid" {
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

// todo - check that working and make sense?
type jsonWithPartitioning struct {
	ConfluentKafka

	generatePartitionKey GeneratePartitionKey
}

func NewJsonWithPartitioning(generatePartitionKey GeneratePartitionKey) kafka.MarshalerUnmarshaler {
	return jsonWithPartitioning{generatePartitionKey: generatePartitionKey}
}

func (j jsonWithPartitioning) Marshal(topic string, msg *message.Message) (*confluentKafka.Message, error) {
	kafkaMsg, err := j.ConfluentKafka.Marshal(topic, msg)
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
