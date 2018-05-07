package eventstore

import (
	"github.com/roblaszczak/gooddd/domain"
	"time"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"encoding/json"
)

type syncKafka struct {
	producer sarama.SyncProducer
}

func NewSimpleSyncKafka(brokers []string) (domain.Eventstore, error) {
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

	return NewSaramaSyncKafka(producer)
}

func NewSaramaSyncKafka(producer sarama.SyncProducer) (domain.Eventstore, error) {
	return syncKafka{producer}, nil
}

// todo - test
func (p syncKafka) Save(events []domain.Event) error {
	var messages []*sarama.ProducerMessage

	for _, event := range events {
		// todo - move to encoder/marshaller (and use it in listener)
		marshalled, err := json.Marshal(event)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal event %s", event)
		}

		messages = append(messages, &sarama.ProducerMessage{
			Topic: "test_topic", // todo - use some strategy for naming?
			Value: sarama.ByteEncoder(marshalled),
		})
	}

	return p.producer.SendMessages(messages)
}
