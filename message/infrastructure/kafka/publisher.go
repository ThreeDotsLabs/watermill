package kafka

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type confluentPublisher struct {
	producer  *kafka.Producer
	marshaler Marshaler

	closed bool
}

func NewPublisher(brokers []string, marshaler Marshaler, kafkaConfigOverwrite kafka.ConfigMap) (message.Publisher, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":            strings.Join(brokers, ","),
		"queue.buffering.max.messages": 10000000,
		"queue.buffering.max.kbytes":   2097151,
		"debug": ",",
	}

	if err := mergeConfluentConfigs(config, kafkaConfigOverwrite); err != nil {
		return nil, err
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create producer")
	}

	return NewCustomPublisher(p, marshaler)
}

func NewCustomPublisher(producer *kafka.Producer, marshaler Marshaler) (message.Publisher, error) {
	return &confluentPublisher{producer, marshaler, false}, nil
}

func (p confluentPublisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	for _, msg := range msgs {
		kafkaMsg, err := p.marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		deliveryChan := make(chan kafka.Event)
		defer close(deliveryChan)

		if err := p.producer.Produce(kafkaMsg, deliveryChan); err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)
		//
		if m.TopicPartition.Error != nil {
			return errors.Wrapf(m.TopicPartition.Error, "delivery of message %s failed", msg.UUID)
		}

	}

	return nil
}

func (p *confluentPublisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	p.producer.Close()

	return nil
}
