package kafka_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
)

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) message.PubSub {
		logger := watermill.NopLogger{}
		marshaler := kafka.DefaultMarshaler{}

		publisher, err := kafka.NewPublisher(brokers, marshaler, nil)
		if err != nil {
			panic(err)
		}

		subscriber, err := kafka.NewConfluentSubscriber(
			kafka.SubscriberConfig{
				Brokers:         brokers,
				ConsumerGroup:   "test",
				AutoOffsetReset: "earliest",
				ConsumersCount:  8,
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
