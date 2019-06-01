package nats_test

import (
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"
	stan "github.com/nats-io/stan.go"
	"github.com/stretchr/testify/require"
)

func newPubSub(t *testing.T, clientID string, queueName string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	natsURL := os.Getenv("WATERMILL_TEST_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	pub, err := nats.NewStreamingPublisher(nats.StreamingPublisherConfig{
		ClusterID: "test-cluster",
		ClientID:  clientID + "_pub",
		Marshaler: nats.GobMarshaler{},
		StanOptions: []stan.Option{
			stan.NatsURL(natsURL),
		},
	}, logger)
	require.NoError(t, err)

	sub, err := nats.NewStreamingSubscriber(nats.StreamingSubscriberConfig{
		ClusterID:        "test-cluster",
		ClientID:         clientID + "_sub",
		QueueGroup:       queueName,
		DurableName:      "durable-name",
		SubscribersCount: 1,
		AckWaitTimeout:   time.Second, // AckTiemout < 5 required for continueAfterErrors
		Unmarshaler:      nats.GobMarshaler{},
		StanOptions: []stan.Option{
			stan.NatsURL(natsURL),
		},
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, watermill.NewUUID(), "test-queue")
}

func createPubSubWithDurable(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, consumerGroup, consumerGroup)
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
		createPubSubWithDurable,
	)
}
