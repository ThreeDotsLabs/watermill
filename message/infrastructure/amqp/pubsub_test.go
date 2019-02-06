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

func createPubSub(t *testing.T) infrastructure.PubSub {
	publisher, err := amqp.NewPublisher(
		amqp.NewDurablePubSubConfig(
			amqpURI,
			nil,
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		amqp.NewDurablePubSubConfig(
			amqpURI,
			amqp.GenerateQueueNameTopicNameWithSuffix("test"),
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber).(infrastructure.PubSub)
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) infrastructure.PubSub {
	publisher, err := amqp.NewPublisher(
		amqp.NewDurablePubSubConfig(
			amqpURI,
			nil,
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		amqp.NewDurablePubSubConfig(
			amqpURI,
			amqp.GenerateQueueNameTopicNameWithSuffix(consumerGroup),
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber).(infrastructure.PubSub)
}

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
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func createQueuePubSub(t *testing.T) infrastructure.PubSub {
	config := amqp.NewDurableQueueConfig(
		amqpURI,
	)

	publisher, err := amqp.NewPublisher(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber).(infrastructure.PubSub)
}

func TestPublishSubscribe_queue(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:        false,
			ExactlyOnceDelivery:   false,
			GuaranteedOrder:       true,
			Persistent:            true,
			RestartServiceCommand: []string{"docker", "restart", "watermill_rabbitmq_1"},
		},
		createQueuePubSub,
		nil,
	)
}

func TestPublishSubscribe_transactional_publish(t *testing.T) {
	config := amqp.NewDurablePubSubConfig(
		amqpURI,
		amqp.GenerateQueueNameTopicNameWithSuffix("test"),
	)
	config.Publish.Transactional = true

	publisher, err := amqp.NewPublisher(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	infrastructure.TestPublishSubscribe(
		t,
		message.NewPubSub(publisher, subscriber).(infrastructure.PubSub),
		infrastructure.Features{
			ConsumerGroups:        true,
			ExactlyOnceDelivery:   false,
			GuaranteedOrder:       true,
			Persistent:            true,
			RestartServiceCommand: []string{"docker", "restart", "watermill_rabbitmq_1"},
		},
	)
}
