package marshal

import (
	"github.com/roblaszczak/gooddd/message"
	"github.com/Shopify/sarama"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka"
)

// todo - unit tests
type Json struct{}

func (Json) Marshal(topic string, msg message.Message) (*sarama.ProducerMessage, error) {
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot serializze msg payload")
	}

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}, nil
}

func (Json) Unmarshal(kafkaMsg *confluentKafka.Message) (message.Message, error) {
	msg := message.NewEmptyDefault()
	if err := json.Unmarshal(kafkaMsg.Value, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

type GeneratePartitionKey func(topic string, msg message.Message) (string, error)

type JsonWithPartitioning struct {
	Json

	generatePartitionKey GeneratePartitionKey
}

func NewJsonWithPartitioning(generatePartitionKey GeneratePartitionKey) kafka.MarshalerUnmarshaler {
	return JsonWithPartitioning{generatePartitionKey: generatePartitionKey}
}

func (j JsonWithPartitioning) Marshal(topic string, msg message.Message) (*sarama.ProducerMessage, error) {
	kafkaMsg, err := j.Json.Marshal(topic, msg)
	if err != nil {
		return nil, err
	}

	key, err := j.generatePartitionKey(topic, msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate partition key")
	}
	kafkaMsg.Key = sarama.StringEncoder(key)

	return kafkaMsg, nil
}
