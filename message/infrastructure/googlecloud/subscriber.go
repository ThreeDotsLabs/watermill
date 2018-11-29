package googlecloud

import (
	"context"
	"sync"

	"google.golang.org/api/option"

	"cloud.google.com/go/pubsub"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed         = errors.New("subscriber is closed")
	ErrSubscriptionDoesNotExist = errors.New("subscription does not exist")
)

type subscriber struct {
	closing chan struct{}
	closed  bool

	allSubscriptionsWaitGroup sync.WaitGroup
	activeSubscriptions       map[string]*pubsub.Subscription
	activeSubscriptionsLock   sync.RWMutex

	client *pubsub.Client
	config SubscriberConfig

	unmarshaler Unmarshaler
	logger      watermill.LoggerAdapter
}

type SubscriberConfig struct {
	SubscriptionName                 string
	ProjectID                        string
	DoNotCreateSubscriptionIfMissing bool

	SubscriptionConfig pubsub.SubscriptionConfig
	ClientOptions      []option.ClientOption
	Unmarshaler        Unmarshaler
}

func (c *SubscriberConfig) setDefaults() {
	if c.Unmarshaler == nil {
		c.Unmarshaler = DefaultUnmarshaler{}
	}
}

func (c SubscriberConfig) Validate() error {
	if c.SubscriptionName == "" {
		return errors.New("SubscriptionName must be set")
	}

	if c.Unmarshaler == nil {
		return errors.New("empty googlecloud message unmarshaler")
	}

	return nil
}

func NewSubscriber(
	ctx context.Context,
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (message.Subscriber, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	client, err := pubsub.NewClient(ctx, config.ProjectID, config.ClientOptions...)
	if err != nil {
		return nil, err
	}

	return &subscriber{
		closing: make(chan struct{}, 1),
		closed:  false,

		allSubscriptionsWaitGroup: sync.WaitGroup{},
		activeSubscriptions:       map[string]*pubsub.Subscription{},
		activeSubscriptionsLock:   sync.RWMutex{},

		client: client,
		config: config,

		unmarshaler: config.Unmarshaler,
		logger:      logger,
	}, nil
}

func (s *subscriber) Subscribe(topic string) (chan *message.Message, error) {
	// todo: pass root ctx from somewhere?
	ctx, cancel := context.WithCancel(context.Background())

	if s.closed {
		return nil, ErrSubscriberClosed
	}

	logFields := watermill.LogFields{
		"provider":          ProviderName,
		"topic":             topic,
		"subscription_name": s.config.SubscriptionName,
	}
	s.logger.Info("Subscribing to Google Cloud PubSub topic", logFields)

	output := make(chan *message.Message, 0)

	s.allSubscriptionsWaitGroup.Add(1)

	sub, err := s.subscription(ctx, topic)
	if err != nil {
		return nil, err
	}

	err = sub.Receive(ctx, func(ctx context.Context, pubsubMsg *pubsub.Message) {
		msg, err := s.unmarshaler.Unmarshal(pubsubMsg)
		if err != nil {
			s.logger.Error("Could not unmarshal Google Cloud PubSub message", err, nil)
			pubsubMsg.Nack()
			return
		}

		select {
		case <-s.closing:
			s.logger.Info(
				"Message not consumed, subscriber is closing",
				logFields,
			)
			pubsubMsg.Nack()
			return
		case output <- msg:
			// message consumed, wait for ack (or nack)
		}

		select {
		case <-msg.Acked():
			pubsubMsg.Ack()
		case <-msg.Nacked():
			pubsubMsg.Nack()
		case <-s.closing:
			pubsubMsg.Nack()
		}
	})

	if err != nil {
		return nil, err
	}

	go func() {
		<-s.closing
		s.logger.Debug("Closing message consumer", logFields)
		cancel()

		close(output)
		s.allSubscriptionsWaitGroup.Done()
	}()

	return output, nil
}

func (s *subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.allSubscriptionsWaitGroup.Wait()

	err := s.client.Close()
	if err != nil {
		return err
	}

	s.logger.Debug("Google Cloud PubSub subscriber closed", nil)
	return nil
}

// subscription obtains a subscription object.
// If subscription doesn't exist on PubSub, create it, unless config variable DoNotCreateSubscriptionWhenMissing is set.
func (s *subscriber) subscription(ctx context.Context, topic string) (sub *pubsub.Subscription, err error) {
	subscriptionName := s.config.SubscriptionName

	s.activeSubscriptionsLock.RLock()
	if sub, ok := s.activeSubscriptions[subscriptionName]; ok {
		return sub, nil
	}
	s.activeSubscriptionsLock.RUnlock()

	sub = s.client.Subscription(subscriptionName)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "could not check if subscription %s exists", subscriptionName)
	}

	if exists {
		return sub, nil
	}

	if s.config.DoNotCreateSubscriptionIfMissing {
		return nil, errors.Wrap(ErrSubscriptionDoesNotExist, subscriptionName)
	}

	t := s.client.Topic(topic)
	exists, err = t.Exists(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "could not check if topic %s exists", topic)
	}

	if !exists {
		return nil, errors.Wrap(ErrTopicDoesNotExist, topic)
	}

	config := s.config.SubscriptionConfig
	config.Topic = t

	return s.client.CreateSubscription(ctx, subscriptionName, s.config.SubscriptionConfig)
}
