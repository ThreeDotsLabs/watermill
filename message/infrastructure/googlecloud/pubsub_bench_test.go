package googlecloud_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) message.PubSub {
		logger := watermill.NopLogger{}

		publisher, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{})
		if err != nil {
			panic(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		subscriber, err := googlecloud.NewSubscriber(
			ctx,
			googlecloud.SubscriberConfig{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return message.NewPubSub(publisher, subscriber)
	})
}
