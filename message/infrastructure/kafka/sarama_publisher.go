package kafka

import (
	"time"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

type saramaPublisher struct {
	producer sarama.SyncProducer

	marshaler Marshaler
}

func NewPublisher(brokers []string, marshaler Marshaler) (message.Publisher, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Return.Successes = true
	config.ClientID = "gooddd"

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create producer")
	}

	return NewCustomPublisher(producer, marshaler)
}

func NewCustomPublisher(producer sarama.SyncProducer, marshaler Marshaler) (message.Publisher, error) {
	return saramaPublisher{producer, marshaler}, nil
}

func (p saramaPublisher) Publish(topic string, messages []message.Message) error {
	var saramaMessages []*sarama.ProducerMessage

	for _, msg := range messages {
		kafkaMsg, err := p.marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg)
		}

		saramaMessages = append(saramaMessages, kafkaMsg)
	}

	return p.producer.SendMessages(saramaMessages)
}

func (p saramaPublisher) ClosePublisher() error {
	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close publisher")
	}

	return nil
}
