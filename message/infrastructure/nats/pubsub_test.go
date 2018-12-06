package nats_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"
)

func newPubSub(t *testing.T, clientID string) message.PubSub {
	logger := watermill.NewStdLogger(true, true)
	pub, err := nats.NewPublisher(nats.PublisherConfig{
		ClusterID: "test-cluster",
		ClientID:  clientID + "_pub", // todo - change
		Marshaler: nats.GobMarshaler{},
	}, logger)
	require.NoError(t, err)

	sub, err := nats.NewSubscriber(nats.SubscriberConfig{
		ClusterID:        "test-cluster",
		ClientID:         clientID + "_sub", // todo - change
		SubscribersCount: 1,
		Unmarshaler:      nats.GobMarshaler{},
	}, logger)
	require.NoError(t, err)

	return message.NewPubSub(pub, sub)
}

func createPubSub(t *testing.T) message.PubSub {
	return newPubSub(t, "foo")
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
		nil,
	)
}
