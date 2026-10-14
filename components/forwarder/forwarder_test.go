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
	"github.com/stretchr/testify/suite"
)

var (
	logger = watermill.NewStdLogger(true, true)

	forwarderTopic = "forwarder_topic"
	outTopic       = "out_topic"
)

func TestForwarder(t *testing.T) {
	suite.Run(t, new(ForwarderSuite))
}

// ForwarderSuite tests forwarding messages from PubSubIn to PubSubOut (which are GoChannel implementation underneath).
type ForwarderSuite struct {
	suite.Suite
	ctx       context.Context
	cancelCtx func()

	publisherIn  PubSubInPublisher
	subscriberIn PubSubInSubscriber

	publisherOut  PubSubOutPublisher
	subscriberOut PubSubOutSubscriber

	decoratedPublisherIn *forwarder.Publisher

	outMessagesCh <-chan *message.Message
}

func (s *ForwarderSuite) SetupTest() {
	// Create test context with a 5 seconds timeout so it will close any subscriptions/handlers running in the background
	// in case of too long test execution.
	s.ctx, s.cancelCtx = context.WithTimeout(context.Background(), time.Second*5)

	// Create a set of publisher and subscribers for both In and Out Pub/Subs.
	s.publisherIn, s.subscriberIn = newPubSubIn()
	s.publisherOut, s.subscriberOut = newPubSubOut()

	s.decoratedPublisherIn = forwarder.NewPublisher(s.publisherIn, forwarder.PublisherConfig{ForwarderTopic: forwarderTopic})
	s.listenOnOutTopic()
}

func (s *ForwarderSuite) TearDownTest() {
	s.NoError(s.publisherIn.Close())
	s.NoError(s.subscriberIn.Close())
	s.NoError(s.publisherOut.Close())
	s.NoError(s.subscriberOut.Close())
	s.cancelCtx()
}

func (s *ForwarderSuite) TestForwarder_publish_using_decorated_publisher() {
	fwd := s.setupForwarder(forwarder.Config{ForwarderTopic: forwarderTopic})
	defer func() {
		s.NoError(fwd.Close())
	}()

	sentMsg := s.sampleMessage()
	err := s.decoratedPublisherIn.Publish(outTopic, sentMsg)
	s.Require().NoError(err)

	s.requireFirstMessage(sentMsg)
}

func (s *ForwarderSuite) TestForwarder_publish_using_non_decorated_publisher() {
	msgAckedDetectorMiddleware, msgAckedCh := s.setupMessageAckedDetectorMiddleware()
	fwd := s.setupForwarder(forwarder.Config{
		ForwarderTopic: forwarderTopic,
		Middlewares:    []message.HandlerMiddleware{msgAckedDetectorMiddleware},
	})
	defer func() {
		s.NoError(fwd.Close())
	}()

	sentMsg := s.sampleMessage()
	err := s.publisherIn.Publish(forwarderTopic, sentMsg)
	s.Require().NoError(err)

	s.requireFirstAckingResult(msgAckedCh, false)
}

func (s *ForwarderSuite) TestForwarder_publish_using_non_decorated_publisher_acking_enabled() {
	msgAckedDetectorMiddleware, msgAckedCh := s.setupMessageAckedDetectorMiddleware()
	fwd := s.setupForwarder(forwarder.Config{
		ForwarderTopic:      forwarderTopic,
		Middlewares:         []message.HandlerMiddleware{msgAckedDetectorMiddleware},
		AckWhenCannotUnwrap: true,
	})
	defer func() {
		s.NoError(fwd.Close())
	}()

	sentMsg := s.sampleMessage()
	err := s.publisherIn.Publish(forwarderTopic, sentMsg)
	s.Require().NoError(err)

	s.requireFirstAckingResult(msgAckedCh, true)
}

// TestForwarder_multiple_forwarders_on_one_router ensures that forwarders sharing a router
// can coexist as long as they have unique handler names.
func TestForwarder_multiple_forwarders_on_one_router(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pubSubIn := gochannel.NewGoChannel(gochannel.Config{}, logger)
	pubSubOut := gochannel.NewGoChannel(gochannel.Config{}, logger)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	handlerNames := []string{"first_forwarder", "second_forwarder"}

	for _, handlerName := range handlerNames {
		_, err := forwarder.NewForwarder(pubSubIn, pubSubOut, logger, forwarder.Config{
			ForwarderTopic: handlerName + "_topic",
			HandlerName:    handlerName,
			Router:         router,
		})
		require.NoError(t, err)
	}

	go func() {
		assert.NoError(t, router.Run(ctx))
	}()
	defer func() {
		assert.NoError(t, router.Close())
	}()

	select {
	case <-router.Running():
	case <-ctx.Done():
		t.Fatal("router not running")
	}

	outMessagesCh, err := pubSubOut.Subscribe(ctx, outTopic)
	require.NoError(t, err)

	for _, handlerName := range handlerNames {
		publisher := forwarder.NewPublisher(pubSubIn, forwarder.PublisherConfig{
			ForwarderTopic: handlerName + "_topic",
		})

		msg := message.NewMessage(watermill.NewUUID(), message.Payload(handlerName))
		require.NoError(t, publisher.Publish(outTopic, msg))
	}

	// Both forwarders publish to the same out topic, so messages can arrive in any order.
	receivedPayloads := map[string]bool{}
	for range handlerNames {
		select {
		case receivedMsg := <-outMessagesCh:
			receivedPayloads[string(receivedMsg.Payload)] = true
			receivedMsg.Ack()
		case <-time.After(time.Second * 3):
			t.Fatal("didn't receive all forwarded messages")
		}
	}

	assert.Equal(t, map[string]bool{"first_forwarder": true, "second_forwarder": true}, receivedPayloads)
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

func newPubSubIn() (PubSubInPublisher, PubSubInSubscriber) {
	channelPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	return PubSubInPublisher{channelPubSub}, PubSubInSubscriber{channelPubSub}
}

func newPubSubOut() (PubSubOutPublisher, PubSubOutSubscriber) {
	channelPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	return PubSubOutPublisher{channelPubSub}, PubSubOutSubscriber{channelPubSub}
}

func (s *ForwarderSuite) setupForwarder(config forwarder.Config) *forwarder.Forwarder {
	f, err := forwarder.NewForwarder(s.subscriberIn, s.publisherOut, logger, config)
	s.Require().NoError(err)

	go func() {
		s.Require().NoError(f.Run(s.ctx))
	}()

	select {
	case <-f.Running():
	case <-s.ctx.Done():
		s.T().Fatal("forwarder not running")
	}

	return f
}

func (s *ForwarderSuite) listenOnOutTopic() {
	var err error
	s.outMessagesCh, err = s.subscriberOut.Subscribe(s.ctx, outTopic)
	s.Require().NoError(err)
}

func (s *ForwarderSuite) requireFirstMessage(expectedMessage *message.Message) {
	select {
	case receivedMessage := <-s.outMessagesCh:
		s.Require().NotNil(receivedMessage)
		s.Require().Truef(receivedMessage.Equals(expectedMessage), "received message: '%s', expected: '%s'", receivedMessage, expectedMessage)
		receivedMessage.Ack()
	case <-time.After(time.Second):
		s.T().Fatal("didn't receive any message after 1 sec")
	}
}

func (s *ForwarderSuite) setupMessageAckedDetectorMiddleware() (message.HandlerMiddleware, <-chan bool) {
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

func (s *ForwarderSuite) requireFirstAckingResult(msgAckedCh <-chan bool, expected bool) {
	select {
	case msgAcked := <-msgAckedCh:
		s.Require().Equal(expected, msgAcked)
	case <-time.After(time.Second):
		s.T().Fatal("acking result not received after 1 sec")
	}
}

func (s *ForwarderSuite) sampleMessage() *message.Message {
	msg := message.NewMessage(watermill.NewUUID(), message.Payload("message payload"))
	msg.Metadata = message.Metadata{"key": "value"}
	return msg
}
