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

func TestPublishSubscribe(t *testing.T) {
	infrastructure.TestPubSub(
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

func TestPublishSubscribe_ordered(t *testing.T) {
	infrastructure.TestPubSub(
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
