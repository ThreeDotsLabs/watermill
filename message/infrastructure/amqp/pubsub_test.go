package amqp_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/amqp"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

var amqpURI = "amqp://guest:guest@localhost:5672/"

func TestPublishSubscribe_pubsub(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:        true,
			ExactlyOnceDelivery:   false,
			GuaranteedOrder:       true,
			Persistent:            true,
			RestartServiceCommand: []string{"docker", "restart", "watermill_rabbitmq_1"},
		},
		func(t *testing.T) message.PubSub {
			pubSub, err := amqp.NewPubSub(
				amqp.NewDurablePubSubConfig(
					amqpURI,
					amqp.GenerateQueueNameTopicNameWithSuffix("test"),
				),
				watermill.NewStdLogger(true, true),
			)
			require.NoError(t, err)

			return pubSub
		},
		func(t *testing.T, consumerGroup string) message.PubSub {
			pubSub, err := amqp.NewPubSub(
				amqp.NewDurablePubSubConfig(
					amqpURI,
					amqp.GenerateQueueNameTopicNameWithSuffix(consumerGroup),
				),
				watermill.NewStdLogger(true, true),
			)
			require.NoError(t, err)

			return pubSub
		},
	)
}

func TestPublishSubscribe_queue(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		func(t *testing.T) message.PubSub {
			pubSub, err := amqp.NewPubSub(
				amqp.NewDurableQueueConfig(amqpURI),
				watermill.NewStdLogger(true, true),
			)
			require.NoError(t, err)

			return pubSub
		},
		nil,
	)
}

// todo - stress
