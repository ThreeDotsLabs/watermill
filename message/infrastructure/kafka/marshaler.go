package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/roblaszczak/gooddd/message"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Marshaler interface {
	Marshal(topic string, msg message.ProducedMessage) (*sarama.ProducerMessage, error)
}

type Unmarshaler interface {
	Unmarshal(*kafka.Message) (message.ConsumedMessage, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}
