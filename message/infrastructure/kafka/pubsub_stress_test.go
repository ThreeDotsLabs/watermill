package kafka_test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/roblaszczak/gooddd/message/infrastructure"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/roblaszczak/gooddd"
	"github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka"
)

func TestPublishSubscribe_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
		},
		func(t *testing.T, consumerGroup string) message.PubSub {
			pubSub, err := kafka.NewPubSub(
				brokers,
				marshal.Json{},
				consumerGroup,
				gooddd.NewStdLogger(true, true),
			)
			require.NoError(t, err)
			return pubSub
		},
	)
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
		},
		func(t *testing.T, consumerGroup string) message.PubSub {
			pubSub, err := kafka.NewPubSub(
				brokers,
				marshal.NewJsonWithPartitioning(generatePartitionKey),
				consumerGroup,
				gooddd.NewStdLogger(true, true),
			)
			require.NoError(t, err)
			return pubSub
		},
	)
}
