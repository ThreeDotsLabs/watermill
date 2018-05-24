package sarama

import (
	"time"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

type marshalMessageFunc func(message *message.Message) ([]byte, error)

type syncKafka struct {
	topic    string
	producer sarama.SyncProducer

	marshalMsg marshalMessageFunc
}

func NewSimpleSyncProducer(topic string, brokers []string, marshalMsg marshalMessageFunc) (message.PublisherBackend, error) {
	// todo - pass consumer id

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create producer")
	}

	return NewSyncProducer(topic, producer, marshalMsg)
}

func NewSyncProducer(topic string, producer sarama.SyncProducer, marshalMsg marshalMessageFunc) (message.PublisherBackend, error) {
	return syncKafka{topic, producer, marshalMsg}, nil
}

// todo - test
func (p syncKafka) Publish(messages []*message.Message) error {
	var saramaMessages []*sarama.ProducerMessage

	for _, message := range messages {
		b, err := p.marshalMsg(message)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", message)
		}

		saramaMessages = append(saramaMessages, &sarama.ProducerMessage{
			Topic: p.topic,
			Value: sarama.ByteEncoder(b),
		})
	}

	return p.producer.SendMessages(saramaMessages)
}

func (p syncKafka) Close() error {
	// todo!
	return nil
}
