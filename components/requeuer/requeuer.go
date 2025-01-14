package requeuer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const RetriesKey = "_watermill_requeuer_retries"

// Requeuer is a component that moves messages from one topic to another.
// It can be used to requeue messages that failed to process.
type Requeuer struct {
	config Config
}

// GeneratePublishTopicParams are the parameters passed to the GeneratePublishTopic function.
type GeneratePublishTopicParams struct {
	Message *message.Message
}

// Config is the configuration for the Requeuer.
type Config struct {
	// Subscriber is the subscriber to consume messages from. Required.
	Subscriber message.Subscriber

	// SubscribeTopic is the topic related to the Subscriber to consume messages from. Required.
	SubscribeTopic string

	// Publisher is the publisher to publish requeued messages to. Required.
	Publisher message.Publisher

	// GeneratePublishTopic is the topic related to the Publisher to publish the requeued message to.
	// For example, it could be a constant, or taken from the message's metadata.
	// Required.
	GeneratePublishTopic func(params GeneratePublishTopicParams) (string, error)

	// Delay is the duration to wait before requeuing the message. Optional.
	// The default is no delay.
	//
	// This can be useful to avoid requeuing messages too quickly, for example, to avoid
	// requeuing a message that failed to process due to a temporary issue.
	//
	// Avoid setting this to a very high value, as it will block the message processing.
	Delay time.Duration

	// Router is the custom router to run the requeue handler on. Optional.
	Router *message.Router
}

func (c *Config) setDefaults(logger watermill.LoggerAdapter) error {
	if c.Router == nil {
		router, err := message.NewRouter(message.RouterConfig{}, logger)
		if err != nil {
			return fmt.Errorf("could not create router: %w", err)
		}

		c.Router = router
	}

	return nil
}

func (c *Config) validate() error {
	if c.Subscriber == nil {
		return errors.New("subscriber is required")
	}

	if c.SubscribeTopic == "" {
		return errors.New("subscribe topic is required")
	}

	if c.Publisher == nil {
		return errors.New("publisher is required")
	}

	if c.GeneratePublishTopic == nil {
		return errors.New("generate publish topic is required")
	}

	return nil
}

// NewRequeuer creates a new Requeuer with the provided Config.
// It's not started automatically. You need to call Run on the returned Requeuer.
func NewRequeuer(
	config Config,
	logger watermill.LoggerAdapter,
) (*Requeuer, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	err := config.setDefaults(logger)
	if err != nil {
		return nil, err
	}

	err = config.validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	r := &Requeuer{
		config: config,
	}

	config.Router.AddNoPublisherHandler(
		"requeuer",
		config.SubscribeTopic,
		config.Subscriber,
		r.handler,
	)

	return r, nil
}

func (r *Requeuer) handler(msg *message.Message) error {
	if r.config.Delay > 0 {
		select {
		case <-msg.Context().Done():
			return msg.Context().Err()
		case <-time.After(r.config.Delay):
		}
	}

	topic, err := r.config.GeneratePublishTopic(GeneratePublishTopicParams{Message: msg})
	if err != nil {
		return err
	}

	retriesStr := msg.Metadata.Get(RetriesKey)
	retries, err := strconv.Atoi(retriesStr)
	if err != nil {
		retries = 0
	}

	retries++

	msg.Metadata.Set(RetriesKey, strconv.Itoa(retries))

	err = r.config.Publisher.Publish(topic, msg)
	if err != nil {
		return err
	}

	return nil
}

// Run runs the Requeuer.
func (r *Requeuer) Run(ctx context.Context) error {
	return r.config.Router.Run(ctx)
}
