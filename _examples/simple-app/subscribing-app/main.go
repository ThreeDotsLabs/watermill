package main

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

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
		message.RouterConfig{},
		logger,
	)
	if err != nil {
		panic(err)
	}

	retryMiddleware := middleware.Retry{}
	retryMiddleware.MaxRetries = 1
	retryMiddleware.WaitTime = time.Millisecond * 10

	poisonQueue, err := middleware.NewPoisonQueue(pub, "poison_queue")
	if err != nil {
		panic(err)
	}

	h.AddMiddleware(
		// limiting processed messages to 10 per second
		middleware.NewThrottle(100, time.Second).Middleware,

		// some, simple metrics
		newMetricsMiddleware().Middleware,

		// retry middleware retries message processing if error occurred in handler
		poisonQueue.Middleware,

		// if retries limit was exceeded, message is sent to poison queue (poison_queue topic)
		retryMiddleware.Middleware,

		// recovered recovers panic from handlers
		middleware.Recoverer,

		// correlation ID middleware adds to every produced message correlation id of consumed message,
		// useful for debugging
		middleware.CorrelationID,

		// simulating error or panic from handler
		middleware.RandomFail(0.01),
		middleware.RandomPanic(0.01),
	)

	// close router when SIGTERM is sent
	h.AddPlugin(plugin.SignalsHandler)

	// handler which just counts added posts
	h.AddHandler(
		"posts_counter",
		"posts_published",
		"posts_count",
		message.NewPubSub(pub, createSubscriber("posts_counter_v2", logger)),
		PostsCounter{memoryCountStorage{new(int64)}}.Count,
	)

	// handler which generates "feed" from events post
	//
	// this implementation just prints it to stdout,
	// but production ready implementation would save posts to some persistent storage
	h.AddNoPublisherHandler(
		"feed_generator",
		"posts_published",
		createSubscriber("feed_generator_v2", logger),
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
			AutoOffsetReset: "earliest",
		},
		marshaler,
		logger,
	)
	if err != nil {
		panic(err)
	}

	return sub
}
