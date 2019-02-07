package kafka_test

import (
	"testing"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
)

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) message.PubSub {
		logger := watermill.NopLogger{}

		publisher, err := kafka.NewPublisher(kafkaBrokers(), kafka.DefaultMarshaler{}, nil, logger)
		if err != nil {
			panic(err)
		}

		saramaConfig := kafka.DefaultSaramaSubscriberConfig()
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

		subscriber, err := kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:       kafkaBrokers(),
				ConsumerGroup: "test",
			},
			saramaConfig,
			kafka.DefaultMarshaler{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return message.NewPubSub(publisher, subscriber)
	})
}
