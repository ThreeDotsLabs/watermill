package googlecloud_test

import (
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
	msgText   = "this is a test message"
	projectID = "googlecloud-test"
)

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

	result := topic.Publish(ctx, msg)
	id, err := result.Get(ctx)
	require.NoError(t, err)
	t.Logf("Published a message with id %s on topic %s", id, topicName)
}

func testMessage() *message.Message {
	return message.NewMessage(uuid.NewV4().String(), message.Payload(msgText))
}
