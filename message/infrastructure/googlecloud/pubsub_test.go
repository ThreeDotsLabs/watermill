package googlecloud_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

const (
	msgText        = "this is a test message"
	projectID      = "googlecloud-test"
	subscriptionID = "test-sub"
)

// TestPubsub tests if the PubSub emulator is up and running correctly.
func TestPubsub(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	client, err := pubsub.NewClient(
		ctx,
		projectID,
	)
	require.NoError(t, err)

	topicName := fmt.Sprintf("test-topic-%d", rand.Int())

	topic, err := client.CreateTopic(ctx, topicName)
	require.NoError(t, err)

	defer func() {
		err := topic.Delete(ctx)
		if err != nil {
			panic(err)
		}
	}()

	msg := &pubsub.Message{
		Data: []byte(msgText),
	}

	msgReceived := make(chan struct{})
	sub := client.Subscription(subscriptionID)
	exists, err := sub.Exists(ctx)
	require.NoError(t, err)

	if !exists {
		sub, err = client.CreateSubscription(ctx, "test-sub", pubsub.SubscriptionConfig{
			Topic: topic,
		})
		require.NoError(t, err)
	}

	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, receivedMsg *pubsub.Message) {
			if bytes.Equal(receivedMsg.Data, msg.Data) {
				msgReceived <- struct{}{}
			}
		})
		require.NoError(t, err)
	}()

	result := topic.Publish(ctx, msg)
	id, err := result.Get(ctx)
	require.NoError(t, err)
	t.Logf("Published a message with id %s on topic %s", id, topicName)

	select {
	case <-msgReceived:
		t.Logf("Message received")
		break
	case <-ctx.Done():
		t.Fatal("test timeout")
	}
}

func testMessage() *message.Message {
	return message.NewMessage(uuid.NewV4().String(), message.Payload(msgText))
}
