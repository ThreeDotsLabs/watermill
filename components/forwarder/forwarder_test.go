package forwarder_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/forwarder"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(true, true)

	forwarderTopic = "forwarder_topic"
	outTopic       = "out_topic"
)

// TestForwarder tests forwarding messages from PubSubIn to PubSubOut (which are GoChannel implementation underneath).
func TestForwarder(t *testing.T) {
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

	messageAckedDetector, messageAckedCh := setupMessageAckedDetectorMiddleware()
	forwarderConfig := forwarder.Config{
		ForwarderTopic: forwarderTopic,
		// Use a middleware to detect if the message was acked by the forwarder.
		Middlewares: []message.HandlerMiddleware{messageAckedDetector},
	}
	// Setup a forwarder to forward messages from Pub/Sub In to Out by passing subscriberIn and publisherOut.
	f := setupForwarder(t, ctx, subscriberIn, publisherOut, logger, forwarderConfig)
	defer func() {
		require.NoError(t, f.Close())
	}()

	outMessages := listenOnOutTopic(t, ctx, subscriberOut, outTopic)

	// Decorate publisherIn so it envelopes messages and publishes them to the forwarder's topic.
	decoratedPublisherIn := f.DecoratePublisher(publisherIn)

	t.Run("publish_using_decorated_publisher", func(t *testing.T) {
		sentMessage := message.NewMessage(watermill.NewUUID(), message.Payload("message payload"))
		sentMessage.Metadata = message.Metadata{"key": "value"}
		err := decoratedPublisherIn.Publish(outTopic, sentMessage)
		require.NoError(t, err)

		// Wait for a message sent using publisherIn on subscriberOut.
		requireFirstMessage(t, sentMessage, outMessages)

		wasMessageForwarded := requireFirstForwardingResult(t, messageAckedCh)
		require.True(t, wasMessageForwarded, "message expected to be forwarded correctly")
	})

	t.Run("publish_using_non_decorated_publisher", func(t *testing.T) {
		// Send a non-enveloped message directly to the forwarder's topic.
		sentMessage := message.NewMessage(watermill.NewUUID(), message.Payload("message payload"))
		sentMessage.Metadata = message.Metadata{"key": "value"}
		err := publisherIn.Publish(forwarderTopic, sentMessage)
		require.NoError(t, err)

		wasMessageForwarded := requireFirstForwardingResult(t, messageAckedCh)
		require.False(t, wasMessageForwarded, "message expected to be not forwarded correctly")
	})

	t.Run("publish_to_empty_topic", func(t *testing.T) {
		sentMessage := message.NewMessage(watermill.NewUUID(), message.Payload("message payload"))
		sentMessage.Metadata = message.Metadata{"key": "value"}
		err := decoratedPublisherIn.Publish("", sentMessage)
		require.Error(t, err)
	})
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

func setupForwarder(t *testing.T, ctx context.Context, subscriberIn PubSubInSubscriber, publisherOut PubSubOutPublisher, logger watermill.LoggerAdapter, config forwarder.Config) *forwarder.Forwarder {
	f, err := forwarder.NewForwarder(subscriberIn, publisherOut, logger, config)
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

func setupMessageAckedDetectorMiddleware() (message.HandlerMiddleware, <-chan bool) {
	messageAckedCh := make(chan bool, 1)
	messageAckedDetector := func(handlerFunc message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			msgs, err := handlerFunc(msg)
			messageAckedCh <- err == nil

			// Always return nil as we don't want to nack the message in tests.
			return msgs, nil
		}
	}

	return messageAckedDetector, messageAckedCh
}

func requireFirstForwardingResult(t *testing.T, messageForwardedCh <-chan bool) bool {
	select {
	case wasMessageForwarded := <-messageForwardedCh:
		return wasMessageForwarded
	case <-time.After(time.Second):
		t.Fatal("forwarding result not received after 1 sec")
	}

	return false
}

func listenOnOutTopic(t *testing.T, ctx context.Context, subscriberOut PubSubOutSubscriber, outTopic string) <-chan *message.Message {
	messagesCh, err := subscriberOut.Subscribe(ctx, outTopic)
	require.NoError(t, err)

	return messagesCh
}

func requireFirstMessage(t *testing.T, expectedMessage *message.Message, ch <-chan *message.Message) {
	select {
	case receivedMessage := <-ch:
		require.NotNil(t, receivedMessage)
		require.Truef(t, receivedMessage.Equals(expectedMessage), "received message: '%s', expected: '%s'", receivedMessage, expectedMessage)
		receivedMessage.Ack()
	case <-time.After(time.Second):
		t.Fatal("didn't receive any message after 1 sec")
	}
}
