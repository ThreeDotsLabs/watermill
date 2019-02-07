package nats_test

import (
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func newPubSub(t *testing.T, clientID string, queueName string) message.PubSub {
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

	return message.NewPubSub(pub, sub)
}

func createPubSub(t *testing.T) infrastructure.PubSub {
	return newPubSub(t, watermill.UUID(), "test-queue").(infrastructure.PubSub)
}

func createPubSubWithDurable(t *testing.T, consumerGroup string) infrastructure.PubSub {
	return newPubSub(t, consumerGroup, consumerGroup).(infrastructure.PubSub)
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
