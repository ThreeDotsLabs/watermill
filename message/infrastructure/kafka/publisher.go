package kafka

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

type confluentPublisher struct {
	producer  *kafka.Producer
	marshaler Marshaler

	closed bool
}

func NewPublisher(brokers []string, marshaler Marshaler) (message.Publisher, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"debug":             ",",
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot create producer")
	}

	return NewCustomPublisher(p, marshaler)
}

func NewCustomPublisher(producer *kafka.Producer, marshaler Marshaler) (message.Publisher, error) {
	return &confluentPublisher{producer, marshaler, false}, nil
}

func (p confluentPublisher) Publish(topic string, msg *message.Message) error {
	kafkaMsg, err := p.marshaler.Marshal(topic, msg)
	if err != nil {
		return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
	}

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	// todo - check that it is blocking?
	if err := p.producer.Produce(kafkaMsg, deliveryChan); err != nil {
		return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	//
	if m.TopicPartition.Error != nil {
		return errors.Wrapf(m.TopicPartition.Error, "delivery of message %s failed", msg.UUID)
	}

	return nil
}

func (p *confluentPublisher) ClosePublisher() error {
	if p.closed {
		return nil
	}
	p.closed = true

	// todo - test that close ensure that all messages are sent
	p.producer.Close()

	return nil
}
