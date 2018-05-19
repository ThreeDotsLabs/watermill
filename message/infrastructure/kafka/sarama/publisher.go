package sarama

import (
	"time"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"encoding/json"
	"github.com/roblaszczak/gooddd/message"
)

type syncKafka struct {
	producer sarama.SyncProducer
}

func NewSimpleSyncProducer(brokers []string) (message.PublisherBackend, error) {
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

	return NewSyncProducer(producer)
}

func NewSyncProducer(producer sarama.SyncProducer) (message.PublisherBackend, error) {
	return syncKafka{producer}, nil
}

// todo - test
func (p syncKafka) Publish(topic string, messages []*message.Message) error {
	var saramaMessages []*sarama.ProducerMessage

	for _, message := range messages {
		// todo - move out of here
		// todo - move to encoder/marshaller (and use it in listener)
		marshalled, err := json.Marshal(message)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", message)
		}

		saramaMessages = append(saramaMessages, &sarama.ProducerMessage{
			Topic: topic, // todo - use some strategy for naming?
			Value: sarama.ByteEncoder(marshalled),
		})
	}

	return p.producer.SendMessages(saramaMessages)
}

func (p syncKafka) Close() error {
	// todo!
	return nil
}
