package kafka_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka/marshal"
)

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) message.PubSub {
		logger := watermill.NopLogger{}
		marshaler := marshal.ConfluentKafka{}

		publisher, err := kafka.NewPublisher(brokers, marshaler, nil)
		if err != nil {
			panic(err)
		}

		subscriber, err := kafka.NewConfluentSubscriber(
			kafka.SubscriberConfig{
				Brokers:        brokers,
				ConsumerGroup:  "test",
				ConsumersCount: 8,
			},
			marshaler,
			logger,
		)
		if err != nil {
			panic(err)
		}

		return message.NewPubSub(publisher, subscriber)
	})
}
