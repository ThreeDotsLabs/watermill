package googlecloud_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := googlecloud.NewPublisher(
			googlecloud.PublisherConfig{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		subscriber, err := googlecloud.NewSubscriber(
			googlecloud.SubscriberConfig{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
