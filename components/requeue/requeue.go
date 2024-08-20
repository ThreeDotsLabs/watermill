package requeue

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const RequeueDelayKey = "requeue_delay"

type Requeue struct {
	router *message.Router
}

type Config struct {
	Subscriber message.Subscriber
	Topic      string
	Publisher  message.Publisher
	Delay      time.Duration
}

func NewRequeue(
	config Config,
	logger watermill.LoggerAdapter,
) (*Requeue, error) {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	router.AddNoPublisherHandler(
		"requeue",
		config.Topic,
		config.Subscriber,
		func(msg *message.Message) error {
			time.Sleep(config.Delay)

			topic := msg.Metadata.Get(middleware.PoisonedTopicKey)
			if topic == "" {
				return errors.New("missing requeue topic")
			}

			err := config.Publisher.Publish(topic, msg)
			if err != nil {
				return err
			}

			return nil
		},
	)

	return &Requeue{
		router: router,
	}, nil
}

func (r *Requeue) Run(ctx context.Context) error {
	return r.router.Run(ctx)
}
