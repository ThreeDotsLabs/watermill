package forwarder_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForwarder tests forwarding messages from PubSubIn to PubSubOut (which are GoChannel implementation underneath).
func TestForwarder(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	// Create a set of publisher and subscribers for both In and Out Pub/Subs.
	publisherIn, subscriberIn := newPubSubIn(logger)
	publisherOut, subscriberOut := newPubSubOut(logger)
	defer func() {
		require.NoError(t, publisherIn.Close())
		require.NoError(t, subscriberIn.Close())
		require.NoError(t, publisherOut.Close())
		require.NoError(t, subscriberOut.Close())
	}()

	// Create test context with a 5 seconds timeout so it will close any subscriptions/handlers running in the background
	// in case of too long test execution.
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelCtx()

	// Setup a forwarder to forward messages from Pub/Sub In to Out by passing subscriberIn and publisherOut.
	f := setupForwarder(t, ctx, subscriberIn, publisherOut, logger)
	defer func() {
		require.NoError(t, f.Close())
	}()

	// Decorate publisherIn so it envelopes messages and publishes them to the forwarder topic.
	decoratedPublisherIn := f.DecoratePublisher(publisherIn)

	outTopic := "out_topic"
	outMessages := listenOnOutTopic(t, ctx, subscriberOut, outTopic)

	// Send a message using decorated publisher.
	sentMessage := message.NewMessage(watermill.NewUUID(), message.Payload("message payload"))
	sentMessage.Metadata = message.Metadata{"key": "value"}
	err := decoratedPublisherIn.Publish(outTopic, sentMessage)
	require.NoError(t, err)

	// Wait for a message sent using publisherIn on subscriberOut.
	receivedMessage := <-outMessages
	require.NotNil(t, receivedMessage)
	receivedMessage.Ack()

	assert.Equal(t, sentMessage.UUID, receivedMessage.UUID)
	assert.Equal(t, sentMessage.Payload, receivedMessage.Payload)
	assert.Equal(t, sentMessage.Metadata, receivedMessage.Metadata)
}

type PubSubInPublisher struct {
	message.Publisher
}
type PubSubInSubscriber struct {
	message.Subscriber
}

type PubSubOutPublisher struct {
	message.Publisher
}
type PubSubOutSubscriber struct {
	message.Subscriber
}

func newPubSubIn(logger watermill.LoggerAdapter) (PubSubInPublisher, PubSubInSubscriber) {
	channelPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	return PubSubInPublisher{channelPubSub}, PubSubInSubscriber{channelPubSub}
}

func newPubSubOut(logger watermill.LoggerAdapter) (PubSubOutPublisher, PubSubOutSubscriber) {
	channelPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	return PubSubOutPublisher{channelPubSub}, PubSubOutSubscriber{channelPubSub}
}

func setupForwarder(t *testing.T, ctx context.Context, subscriberIn PubSubInSubscriber, publisherOut PubSubOutPublisher, logger watermill.LoggerAdapter) *forwarder.Forwarder {
	forwarderConfig := forwarder.Config{ForwarderTopic: "forwarder_topic"}
	f, err := forwarder.NewForwarder(subscriberIn, publisherOut, logger, forwarderConfig)
	require.NoError(t, err)

	go func() {
		require.NoError(t, f.Run(ctx))
	}()

	select {
	case <-f.Running():
	case <-ctx.Done():
		t.Error("forwarder not running")
	}

	return f
}

func listenOnOutTopic(t *testing.T, ctx context.Context, subscriberOut PubSubOutSubscriber, outTopic string) <-chan *message.Message {
	messagesCh, err := subscriberOut.Subscribe(ctx, outTopic)
	require.NoError(t, err)

	return messagesCh
}
