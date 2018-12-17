package kafka_test

import (
	"testing"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/stretchr/testify/require"
)

var brokers = []string{"localhost:9092"}

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) message.PubSub {
	logger := watermill.NewStdLogger(true, true)

	publisher, err := kafka.NewPublisher(brokers, marshaler, nil, logger)
	require.NoError(t, err)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       brokers,
			ConsumerGroup: consumerGroup,
		},
		saramaConfig,
		marshaler,
		logger,
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func generatePartitionKey(topic string, msg *message.Message) (string, error) {
	return msg.Metadata.Get("partition_key"), nil
}

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) message.PubSub {
	return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup)
}

func createPubSub(t *testing.T) message.PubSub {
	return createPubSubWithConsumerGrup(t, "test")
}

func createPartitionedPubSub(t *testing.T) message.PubSub {
	return newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test")
}

func createNoGroupSubscriberConstructor(t *testing.T) message.Subscriber {
	logger := watermill.NewStdLogger(true, true)

	marshaler := kafka.DefaultMarshaler{}

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	sub, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       brokers,
			ConsumerGroup: "",
		},
		saramaConfig,
		marshaler,
		logger,
	)
	require.NoError(t, err)

	return sub
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
		createPubSubWithConsumerGrup,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createPartitionedPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	infrastructure.TestNoGroupSubscriber(t, createPubSub, createNoGroupSubscriberConstructor)
}
