package googlecloud_test

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=googlecloud:8085 for this to work

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) message.PubSub {
		ctx := context.Background()
		logger := watermill.NopLogger{}

		publisher, err := googlecloud.NewPublisher(ctx, googlecloud.PublisherConfig{})

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
