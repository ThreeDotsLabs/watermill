package pubsub_test

import (
	"testing"
	"github.com/roblaszczak/gooddd/pubsub"
	"github.com/roblaszczak/gooddd/pubsub/infrastructure/gochannel"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
	"github.com/satori/go.uuid"
)

func TestPubSub(t *testing.T) {
	pubSubs := []pubsub.PubSub{
		gochannel.NewPubSub(),
	}

	// comment it out for easier debug
	t.Parallel()

	for _, p := range pubSubs {
		t.Run(fmt.Sprintf("%t", p), func(t *testing.T) {
			testPubSub_send_to_one_consumer(t, p)
			testPubSub_read_already_sent_message(t, p)
		})
	}
}

func testPubSub_send_to_one_consumer(t *testing.T, p pubsub.PubSub) {
	topic := generateTopicName("testPubSub_send_to_one_consumer")
	msg := "foo"

	ch, err := p.Subscribe(topic)
	require.NoError(t, err)
	receivedMsg := ""
	go func() {
		received := <-ch
		receivedMsg = received.(string)
	}()

	p.Publish(msg)
	assert.Equal(t, msg, receivedMsg)
}

func testPubSub_read_already_sent_message(t *testing.T, p pubsub.PubSub) {
	msg := "foo"
	p.Publish(msg)

	ch, err := p.Subscribe("")
	require.NoError(t, err)
	receivedMsg := ""
	go func() {
		received := <-ch
		receivedMsg = received.(string)
	}()

	assert.Equal(t, msg, receivedMsg)
}

func generateTopicName(prefix string) string {
	return prefix + "_" + uuid.NewV1().String()
}
