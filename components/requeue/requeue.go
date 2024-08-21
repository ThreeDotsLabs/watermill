package requeue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const RequeueRetriesKey = "requeue_retries"

type Requeue struct {
	config Config
	router *message.Router
}

type Config struct {
	// Subscriber is the subscriber to consume messages from. Required.
	Subscriber message.Subscriber

	// SubscribeTopic is the topic related to the Subscriber to consume messages from. Required.
	SubscribeTopic string

	// Publisher is the publisher to publish requeued messages to. Required.
	Publisher message.Publisher

	// PublishTopic is the topic related to the Publisher to publish requeued messages to.
	// Defaults to the original topic of the message that was requeued, taken from the poison queue middleware metadata.
	PublishTopic string

	// Delay is the duration to wait before requeueing the message. Optional.
	Delay time.Duration

	// Router is the custom router to run the requeue handler on. Optional.
	Router *message.Router
}

func (c *Config) setDefaults() {
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

	return nil
}

func NewRequeue(
	config Config,
	logger watermill.LoggerAdapter,
) (*Requeue, error) {
	config.setDefaults()
	err := config.validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	var router *message.Router
	if config.Router == nil {
		router, err = message.NewRouter(message.RouterConfig{}, logger)
		if err != nil {
			return nil, fmt.Errorf("could not create router: %w", err)
		}
	} else {
		router = config.Router
	}

	r := &Requeue{
		config: config,
		router: router,
	}

	router.AddNoPublisherHandler(
		"requeue",
		config.SubscribeTopic,
		config.Subscriber,
		r.handler,
	)

	return r, nil
}

func (r *Requeue) handler(msg *message.Message) error {
	if r.config.Delay > 0 {
		time.Sleep(r.config.Delay)
	}

	var topic string
	if r.config.PublishTopic == "" {
		topic = msg.Metadata.Get(middleware.PoisonedTopicKey)
		if topic == "" {
			return errors.New("missing requeue topic")
		}
	} else {
		topic = r.config.PublishTopic
	}

	retriesStr := msg.Metadata.Get(RequeueRetriesKey)
	retries, err := strconv.Atoi(retriesStr)
	if err != nil {
		retries = 0
	}

	retries++

	msg.Metadata.Set(RequeueRetriesKey, strconv.Itoa(retries))

	err = r.config.Publisher.Publish(topic, msg)
	if err != nil {
		return err
	}

	return nil
}

func (r *Requeue) Run(ctx context.Context) error {
	return r.router.Run(ctx)
}
