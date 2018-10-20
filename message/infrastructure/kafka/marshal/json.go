package marshal

import (
	"github.com/roblaszczak/gooddd/message"
	"github.com/Shopify/sarama"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka"
)

type jsonMessage struct {
	MessageUUID     string            `json:"message_uuid"`
	MessageMetadata map[string]string `json:"message_metadata"`
}

func (j jsonMessage) UUID() string {
	return j.MessageUUID
}

func (j jsonMessage) GetMetadata(key string) string {
	if val, ok := j.MessageMetadata[key]; ok {
		return val
	}

	return ""
}

func (j jsonMessage) AllMetadata() map[string]string {
	return j.MessageMetadata
}

type jsonProducedMessage struct {
	jsonMessage

	MessagePayload message.Payload `json:"message_payload"`
}

type jsonConsumedMessage struct {
	jsonMessage

	payloadData []byte
	*message.Ack
}

func (j jsonConsumedMessage) UnmarshalPayload(val interface{}) error {
	m := struct{ MessagePayload message.Payload `json:"message_payload"` }{val} // todo - use jsonMessage??

	if err := json.Unmarshal(j.payloadData, &m); err != nil {
		return errors.Wrap(err, "cannot unmarshal jsonMessage payload")
	}

	return nil
}

type Json struct{}

func (Json) Marshal(topic string, msg message.ProducedMessage) (*sarama.ProducerMessage, error) {
	j := jsonProducedMessage{
		jsonMessage: jsonMessage{
			MessageUUID:     msg.UUID(),
			MessageMetadata: msg.AllMetadata(),
		},
		MessagePayload: msg.Payload(),
	}

	data, err := json.Marshal(j)
	if err != nil {
		return nil, errors.Wrap(err, "cannot serialize message to json")
	}

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}, nil
}

func (Json) Unmarshal(kafkaMsg *confluentKafka.Message) (message.ConsumedMessage, error) {
	j := jsonConsumedMessage{}
	if err := json.Unmarshal(kafkaMsg.Value, &j); err != nil {
		return nil, err
	}
	j.payloadData = kafkaMsg.Value
	j.Ack = message.NewAck() // todo - test in some way

	return j, nil
}

type GeneratePartitionKey func(topic string, msg message.ProducedMessage) (string, error)

type jsonWithPartitioning struct {
	Json

	generatePartitionKey GeneratePartitionKey
}

func NewJsonWithPartitioning(generatePartitionKey GeneratePartitionKey) kafka.MarshalerUnmarshaler {
	return jsonWithPartitioning{generatePartitionKey: generatePartitionKey}
}

func (j jsonWithPartitioning) Marshal(topic string, msg message.ProducedMessage) (*sarama.ProducerMessage, error) {
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
