package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/roblaszczak/gooddd/message"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Marshaller interface {
	Marshal(topic string, msg message.Message) (*sarama.ProducerMessage, error)
}

type Unmarshaller interface {
	Unmarshal(*kafka.Message) (message.Message, error)
}

type MarshallerUnmarshaller interface {
	Marshaller
	Unmarshaller
}
