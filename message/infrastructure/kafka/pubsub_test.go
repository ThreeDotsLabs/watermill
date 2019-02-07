package kafka_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/stretchr/testify/require"
)

func kafkaBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9092"}
}

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string) message.PubSub {
	logger := watermill.NewStdLogger(true, true)

	publisher, err := kafka.NewPublisher(kafkaBrokers(), marshaler, nil, logger)
	require.NoError(t, err)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	saramaConfig.Admin.Timeout = time.Second * 30
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.ChannelBufferSize = 10240
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Millisecond * 500
	saramaConfig.Consumer.Group.Rebalance.Timeout = time.Millisecond * 500

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       kafkaBrokers(),
			ConsumerGroup: consumerGroup,
			InitializeTopicDetails: &sarama.TopicDetail{
				NumPartitions:     8,
				ReplicationFactor: 1,
			},
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

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) infrastructure.PubSub {
	return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup).(infrastructure.PubSub)
}

func createPubSub(t *testing.T) infrastructure.PubSub {
	return createPubSubWithConsumerGrup(t, "test").(infrastructure.PubSub)
}

func createPartitionedPubSub(t *testing.T) infrastructure.PubSub {
	return newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test").(infrastructure.PubSub)
}

func createNoGroupSubscriberConstructor(t *testing.T) message.Subscriber {
	logger := watermill.NewStdLogger(true, true)

	marshaler := kafka.DefaultMarshaler{}

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	sub, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       kafkaBrokers(),
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
	features := infrastructure.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	if testing.Short() {
		// Kafka tests are a bit slow, so let's run only basic test
		// todo - speed up
		t.Log("Running only TestPublishSubscribe for Kafka with -short flag")
		infrastructure.TestPublishSubscribe(t, createPubSub(t), features)
		return
	}

	infrastructure.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

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
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	infrastructure.TestNoGroupSubscriber(t, createPubSub, createNoGroupSubscriberConstructor)
}
