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
		return "", err
	}
	return fmt.Sprintf("%d", payload.Type), nil
}

func TestPublishSubscribe(t *testing.T) {
	pubsub, err := kafka.NewPubSub(
		brokers,
		marshal.Json{},
		"tests",
		gooddd.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	infrastructure.PublishSubscribeTest(t, pubsub)
}

func TestPublishSubscribeInOrder(t *testing.T) {
	pubsub, err := kafka.NewPubSub(
		brokers,
		marshal.NewJsonWithPartitioning(generatePartitionKey),
		"tests",
		gooddd.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	infrastructure.PublishSubscribeInOrderTest(t, pubsub)
}

func TestPublishSubscribe_resend_on_error(t *testing.T) {
	pubsub, err := kafka.NewPubSub(
		brokers,
		marshal.Json{},
		"tests",
		gooddd.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	infrastructure.PublishSubscribeTest_resend_on_error(t, pubsub)
}

func TestPublishSubscribe_no_ack(t *testing.T) {
	pubsub, err := kafka.NewPubSub(
		brokers,
		marshal.NewJsonWithPartitioning(generatePartitionKey),
		"tests",
		gooddd.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	infrastructure.PublishSubscribeTest_no_ack(t, pubsub)
}

func TestPublishSubscribe_continue_after_close(t *testing.T) {
	infrastructure.PublishSubscribeTest_continue_after_close(t, func(t *testing.T) message.PubSub {
		pubsub, err := kafka.NewPubSub(
			brokers,
			marshal.Json{},
			"tests",
			gooddd.NewStdLogger(true, true),
		)
		require.NoError(t, err)

		return pubsub
	})
}
