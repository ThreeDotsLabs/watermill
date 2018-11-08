package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/roblaszczak/gooddd/message"
)

type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*kafka.Message, error)
}

type Unmarshaler interface {
	Unmarshal(*kafka.Message) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}
