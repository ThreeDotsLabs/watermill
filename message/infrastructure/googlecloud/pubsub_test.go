package googlecloud_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"cloud.google.com/go/pubsub"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
	"github.com/stretchr/testify/require"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

func newPubSub(t *testing.T, marshaler googlecloud.MarshalerUnmarshaler, subscriptionName string) message.PubSub {
	ctx := context.Background()
	publisher, err := googlecloud.NewPublisher(
		ctx,
		googlecloud.PublisherConfig{
			Marshaler: marshaler,
		},
	)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, true)

	subscriber, err := googlecloud.NewSubscriber(
		ctx,
		googlecloud.SubscriberConfig{
			SubscriptionName: subscriptionName,
			SubscriptionConfig: pubsub.SubscriptionConfig{
				RetainAckedMessages: false,
			},
			Unmarshaler: marshaler,
		},
		logger,
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func createPubSubWithSubscriptionName(t *testing.T, subscriptionName string) message.PubSub {
	return newPubSub(t, googlecloud.DefaultMarshalerUnmarshaler{}, subscriptionName)
}

func createPubSub(t *testing.T) message.PubSub {
	return createPubSubWithSubscriptionName(t, fmt.Sprintf("test_%d", rand.Int()))
}

func TestPublishSubscribe(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithSubscriptionName,
	)
}
