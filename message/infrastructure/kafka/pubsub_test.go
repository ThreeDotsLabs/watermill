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

func generatePartitionKey(topic string, msg *message.Message) (string, error) {
	return "", nil // todo - fix
	//payload := infrastructure.MessageWithType{}
	//if err := msg.UnmarshalPayload(&payload); err != nil {
	//	return "", nil
	//}
	//
	//return fmt.Sprintf("%d", payload.Type), nil
}

func createPubSub(t *testing.T) message.PubSub {
	marshaler := marshal.ConfluentKafka{}

	publisher, err := kafka.NewPublisher(brokers, marshaler)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, true)

	subscriber, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:        brokers,
			ConsumersCount: 8,
		},
		marshaler,
		logger,
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func createPartitionedPubSub(t *testing.T) message.PubSub {
	marshaler := marshal.NewJsonWithPartitioning(generatePartitionKey)

	publisher, err := kafka.NewPublisher(brokers, marshaler)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, true)

	subscriber, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:        brokers,
			ConsumersCount: 8,
		},
		marshaler, logger,
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func createNoGroupSubscriberConstructor(t *testing.T) message.NoConsumerGroupSubscriber {
	logger := watermill.NewStdLogger(true, true)

	marshaler := marshal.ConfluentKafka{}

	sub, err := kafka.NewNoConsumerGroupSubscriber(
		kafka.SubscriberConfig{
			Brokers:        brokers,
			ConsumersCount: 1,
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

func TestNoGroupSubscriber(t *testing.T) {
	infrastructure.TestNoGroupSubscriber(t, createPubSub, createNoGroupSubscriberConstructor)
}
