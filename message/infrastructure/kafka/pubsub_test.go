package kafka_test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/roblaszczak/gooddd/message/infrastructure"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/roblaszczak/gooddd"
	"github.com/roblaszczak/gooddd/message"
	"fmt"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka"
)

var brokers = []string{"localhost:9092"}

func generatePartitionKey(topic string, msg message.Message) (string, error) {
	payload := infrastructure.MessageWithType{}
	if err := msg.UnmarshalPayload(&payload); err != nil {
		return "", nil
	}

	return fmt.Sprintf("%d", payload.Type), nil
}

func createPubSub(t *testing.T, consumerGroup string) message.PubSub {
	marshaler := marshal.Json{}

	publisher, err := kafka.NewPublisher(brokers, marshaler)
	require.NoError(t, err)

	logger := gooddd.NewStdLogger(true, true)

	subscriber, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:        brokers,
			ConsumerGroup:  consumerGroup,
			ConsumersCount: 8,
		},
		marshaler,
		logger,
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func createPartitionedPubSub(t *testing.T, consumerGroup string) message.PubSub {
	marshaler := marshal.NewJsonWithPartitioning(generatePartitionKey)

	publisher, err := kafka.NewPublisher(brokers, marshaler)
	require.NoError(t, err)

	logger := gooddd.NewStdLogger(true, true)

	subscriber, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:        brokers,
			ConsumerGroup:  consumerGroup,
			ConsumersCount: 8,
		},
		marshaler, logger,
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func TestPublishSubscribe(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
		},
		createPubSub,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
		},
		createPartitionedPubSub,
	)
}
