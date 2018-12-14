package nats_test

import (
	"testing"
	"time"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"
)

func newPubSub(t *testing.T, clientID string, queueName string) message.PubSub {
	logger := watermill.NewStdLogger(true, true)
	pub, err := nats.NewStreamingPublisher(nats.StreamingPublisherConfig{
		ClusterID: "test-cluster",
		ClientID:  clientID + "_pub",
		Marshaler: nats.GobMarshaler{},
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
	}, logger)
	require.NoError(t, err)

	return message.NewPubSub(pub, sub)
}

func createPubSub(t *testing.T) message.PubSub {
	return newPubSub(t, uuid.NewV4().String(), "test-queue")
}

func createPubSubWithDurable(t *testing.T, consumerGroup string) message.PubSub {
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
