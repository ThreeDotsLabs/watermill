package main

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
)

type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`
	Author     string    `json:"author"`
	Title      string    `json:"title"`
}

var (
	marshaler = kafka.DefaultMarshaler{}
	brokers   = []string{"localhost:9092"}

	logger = watermill.NewStdLogger(false, false)
)

func main() {
	pub, err := kafka.NewPublisher(brokers, marshaler, nil)
	if err != nil {
		panic(err)
	}

	h, err := message.NewRouter(
		message.RouterConfig{
			ServerName: "simple-app",
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	retryMiddleware := middleware.Retry{}
	retryMiddleware.MaxRetries = 1
	retryMiddleware.WaitTime = time.Second

	poisonQueue, err := middleware.NewPoisonQueue(pub, "poison_queue")
	if err != nil {
		panic(err)
	}

	h.AddMiddleware(
		newMetricsMiddleware().Middleware,
		poisonQueue.Middleware,
		retryMiddleware.Middleware,
		middleware.Recoverer,
		middleware.CorrelationID,
		middleware.RandomFail(0.1),
		middleware.RandomPanic(0.1),
	)
	h.AddPlugin(plugin.SignalsHandler)

	h.AddHandler(
		"posts_counter",
		"app1-posts_published",
		"posts_count",
		message.NewPubSub(pub, createSubscriber("app2-posts_counter_v2", logger)),
		PostsCounter{memoryCountStorage{new(int64)}}.Count,
	)
	h.AddNoPublisherHandler(
		"feed_generator",
		"app1-posts_published",
		createSubscriber("app2-feed_generator_v2", logger),
		FeedGenerator{printFeedStorage{}}.UpdateFeed,
	)

	h.Run()
}

func createSubscriber(consumerGroup string, logger watermill.LoggerAdapter) message.Subscriber {
	sub, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:        brokers,
			ConsumerGroup:  consumerGroup,
			ConsumersCount: 8,
		},
		marshaler,
		logger,
	)
	if err != nil {
		panic(err)
	}

	return sub
}
