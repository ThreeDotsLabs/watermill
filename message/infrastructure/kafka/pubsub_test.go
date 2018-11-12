package kafka_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka/marshal"
	"github.com/stretchr/testify/require"
)

var brokers = []string{"localhost:9092"}

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) message.PubSub {
	publisher, err := kafka.NewPublisher(brokers, marshaler, nil)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, true)

	subscriber, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:         brokers,
			ConsumerGroup:   consumerGroup,
			AutoOffsetReset: "earliest",
			ConsumersCount:  8,
		},
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
	return newPubSub(t, marshal.ConfluentKafka{}, consumerGroup)
}

func createPubSub(t *testing.T) message.PubSub {
	return createPubSubWithConsumerGrup(t, "test")
}

func createPartitionedPubSub(t *testing.T) message.PubSub {
	return newPubSub(t, marshal.NewKafkaJsonWithPartitioning(generatePartitionKey), "test")
}

func createNoGroupSubscriberConstructor(t *testing.T) message.Subscriber {
	logger := watermill.NewStdLogger(true, true)

	marshaler := marshal.ConfluentKafka{}

	sub, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:         brokers,
			NoConsumerGroup: true,
			AutoOffsetReset: "earliest",
			ConsumersCount:  1,
		},
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
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPartitionedPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	infrastructure.TestNoGroupSubscriber(t, createPubSub, createNoGroupSubscriberConstructor)
}
