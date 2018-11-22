package googlecloud

import (
	"context"
	"runtime"
	"sync"

	"google.golang.org/api/option"

	"cloud.google.com/go/pubsub"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

type subscriber struct {
	closing chan struct{}
	closed  bool

	allSubscriptionsWaitGroup sync.WaitGroup

	client *pubsub.Client
	config SubscriberConfig

	unmarshaler Unmarshaler
	logger      watermill.LoggerAdapter
}

type SubscriberConfig struct {
	SubscriptionName            string
	ProjectID                   string
	CreateSubscriptionIfMissing bool

	ConsumersCount int

	SubscriptionConfig pubsub.SubscriptionConfig
	ClientOptions      []option.ClientOption
	Unmarshaler        Unmarshaler
}

func (c *SubscriberConfig) setDefaults() {
	if c.ConsumersCount == 0 {
		c.ConsumersCount = runtime.NumCPU()
	}

	if c.Unmarshaler == nil {
		c.Unmarshaler = DefaultUnmarshaler{}
	}
}

func (c SubscriberConfig) Validate() error {
	if c.ConsumersCount <= 0 {
		return errors.Errorf("ConsumersCount must be greater than 0, have %d", c.ConsumersCount)
	}
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
		client:      client,
		config:      config,
		unmarshaler: config.Unmarshaler,
		logger:      logger,
	}, nil
}

func (s *subscriber) Subscribe(topic string) (chan *message.Message, error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	logFields := watermill.LogFields{
		"provider":          ProviderName,
		"topic":             topic,
		"consumers_count":   s.config.ConsumersCount,
		"subscription_name": s.config.SubscriptionName,
	}
	s.logger.Info("Subscribing to Google Cloud PubSub topic", logFields)

	output := make(chan *message.Message, 0)

	// todo: actually subscribe and consume the messages :)

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
