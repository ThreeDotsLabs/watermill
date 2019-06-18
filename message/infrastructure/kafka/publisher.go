package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	producer  sarama.SyncProducer
	marshaler Marshaler

	logger watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new Kafka Publisher.
func NewPublisher(
	brokers []string,
	marshaler Marshaler,
	overwriteSaramaConfig *sarama.Config,
	logger watermill.LoggerAdapter,
) (message.Publisher, error) {
	if overwriteSaramaConfig == nil {
		overwriteSaramaConfig = DefaultSaramaSyncPublisherConfig()
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	producer, err := sarama.NewSyncProducer(brokers, overwriteSaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka producer")
	}

	return &Publisher{producer, marshaler, logger, false}, nil
}

func DefaultSaramaSyncPublisherConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Version = sarama.V1_0_0_0
	config.Metadata.Retry.Backoff = time.Second * 2
	config.ClientID = "watermill"

	return config
}

// Publish publishes message to Kafka.
//
// Publish is blocking and wait for ack from Kafka.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		kafkaMsg, err := p.marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		partition, offset, err := p.producer.SendMessage(kafkaMsg)
		if err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		logFields["kafka_partition"] = partition
		logFields["kafka_partition_offset"] = offset

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka producer")
	}

	return nil
}
