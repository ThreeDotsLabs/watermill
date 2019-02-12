package googlecloud_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=googlecloud:8085 for this to work

func newPubSub(t *testing.T, marshaler googlecloud.MarshalerUnmarshaler, subscriptionName googlecloud.SubscriptionNameFn) message.PubSub {
	ctx := context.Background()
	publisher, err := googlecloud.NewPublisher(
		ctx,
		googlecloud.PublisherConfig{
			Marshaler: marshaler,
		},
	)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, true)

	subscriber, err := googlecloud.NewSubscriber(
		ctx,
		googlecloud.SubscriberConfig{
			GenerateSubscriptionName: subscriptionName,
			SubscriptionConfig: pubsub.SubscriptionConfig{
				RetainAckedMessages: false,
			},
			Unmarshaler: marshaler,
		},
		logger,
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func createPubSubWithSubscriptionName(t *testing.T, subscriptionName string) infrastructure.PubSub {
	return newPubSub(t, googlecloud.DefaultMarshalerUnmarshaler{},
		googlecloud.TopicSubscriptionNameWithSuffix(subscriptionName),
	).(infrastructure.PubSub)
}

func createPubSub(t *testing.T) infrastructure.PubSub {
	return newPubSub(t, googlecloud.DefaultMarshalerUnmarshaler{}, googlecloud.TopicSubscriptionName).(infrastructure.PubSub)
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
		createPubSubWithSubscriptionName,
	)
}

func TestSubscriberUnexpectedTopicForSubscription(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	rand.Seed(time.Now().Unix())
	testNumber := rand.Int()
	logger := watermill.NewStdLogger(true, true)

	subNameFn := func(topic string) string {
		return fmt.Sprintf("sub_%d", testNumber)
	}

	sub1, err := googlecloud.NewSubscriber(ctx, googlecloud.SubscriberConfig{
		GenerateSubscriptionName: subNameFn,
	}, logger)
	require.NoError(t, err)

	topic1 := fmt.Sprintf("topic1_%d", testNumber)

	sub2, err := googlecloud.NewSubscriber(ctx, googlecloud.SubscriberConfig{
		GenerateSubscriptionName: subNameFn,
	}, logger)
	require.NoError(t, err)
	topic2 := fmt.Sprintf("topic2_%d", testNumber)

	howManyMessages := 100

	messagesTopic1, err := sub1.Subscribe(context.Background(), topic1)
	require.NoError(t, err)

	allMessagesReceived := make(chan struct{})
	go func() {
		defer close(allMessagesReceived)
		messagesReceived := 0
		for range messagesTopic1 {
			messagesReceived++
			if messagesReceived == howManyMessages {
				return
			}
		}
	}()

	produceMessages(t, ctx, topic1, howManyMessages)

	select {
	case <-allMessagesReceived:
		t.Log("All topic 1 messages received")
	case <-ctx.Done():
		t.Fatal("Test timed out")
	}

	_, err = sub2.Subscribe(context.Background(), topic2)
	require.Equal(t, googlecloud.ErrUnexpectedTopic, errors.Cause(err))
}

func produceMessages(t *testing.T, ctx context.Context, topic string, howMany int) {
	pub, err := googlecloud.NewPublisher(ctx, googlecloud.PublisherConfig{})
	require.NoError(t, err)
	defer pub.Close()

	messages := make([]*message.Message, howMany)
	for i := 0; i < howMany; i++ {
		messages[i] = message.NewMessage(watermill.NewUUID(), []byte{})
	}

	require.NoError(t, pub.Publish(topic, messages...))
}
