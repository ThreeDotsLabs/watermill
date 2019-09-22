package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	marshaler = kafka.DefaultMarshaler{}
	brokers   = []string{"kafka:9092"}
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	logger.Info("Starting the consumer", nil)

	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: marshaler,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	r, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	retryMiddleware := middleware.Retry{
		MaxRetries:      1,
		InitialInterval: time.Millisecond * 10,
	}

	poisonQueue, err := middleware.PoisonQueue(pub, "poison_queue")
	if err != nil {
		panic(err)
	}

	r.AddMiddleware(
		// Recoverer middleware recovers panic from handlers and middlewares
		middleware.Recoverer,

		// Limit incoming messages to 10 per second
		middleware.NewThrottle(10, time.Second).Middleware,

		// If the retries limit is exceeded (see retryMiddleware below), the message is sent
		// to the poison queue (published to poison_queue topic)
		poisonQueue,

		// Retry middleware retries message processing if an error occurred in the handler
		retryMiddleware.Middleware,

		// Correlation ID middleware adds the correlation ID of the consumed message to each produced message.
		// It's useful for debugging.
		middleware.CorrelationID,

		// Simulate errors or panics from handler
		middleware.RandomFail(0.01),
		middleware.RandomPanic(0.01),
	)

	// Close the router when a SIGTERM is sent
	r.AddPlugin(plugin.SignalsHandler)

	// Handler that counts consumed posts
	r.AddHandler(
		"posts_counter",
		"posts_published",
		createSubscriber("posts_counter", logger),
		"posts_count",
		pub,
		PostsCounter{memoryCountStorage{new(int64)}}.Count,
	)

	// Handler that generates "feed" from consumed posts
	//
	// This implementation just prints the posts on stdout,
	// but production ready implementation would save posts to some persistent storage.
	r.AddNoPublisherHandler(
		"feed_generator",
		"posts_published",
		createSubscriber("feed_generator", logger),
		FeedGenerator{printFeedStorage{}}.UpdateFeed,
	)

	if err = r.Run(context.Background()); err != nil {
		panic(err)
	}
}

func createSubscriber(consumerGroup string, logger watermill.LoggerAdapter) message.Subscriber {
	sub, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       brokers,
			Unmarshaler:   marshaler,
			ConsumerGroup: consumerGroup,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return sub
}

type postsCountUpdated struct {
	NewCount int64 `json:"new_count"`
}

type countStorage interface {
	CountAdd() (int64, error)
	Count() (int64, error)
}

type memoryCountStorage struct {
	count *int64
}

func (m memoryCountStorage) CountAdd() (int64, error) {
	return atomic.AddInt64(m.count, 1), nil
}

func (m memoryCountStorage) Count() (int64, error) {
	return atomic.LoadInt64(m.count), nil
}

type PostsCounter struct {
	countStorage countStorage
}

func (p PostsCounter) Count(msg *message.Message) ([]*message.Message, error) {
	// When implementing counter for production use, you'd probably need to add some kind of deduplication here,
	// unless the used Pub/Sub supports exactly-once delivery.

	newCount, err := p.countStorage.CountAdd()
	if err != nil {
		return nil, errors.Wrap(err, "cannot add count")
	}

	producedMsg := postsCountUpdated{NewCount: newCount}
	b, err := json.Marshal(producedMsg)
	if err != nil {
		return nil, err
	}

	return []*message.Message{message.NewMessage(watermill.NewUUID(), b)}, nil
}

// postAdded might look similar to the postAdded type from producer.
// It's intentionally not imported here. We avoid coupling the services at the cost of duplication.
// We don't need all of it's data either (content is not displayed on the feed).
type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`
	Author     string    `json:"author"`
	Title      string    `json:"title"`
}

type feedStorage interface {
	AddToFeed(title, author string, time time.Time) error
}

type printFeedStorage struct{}

func (printFeedStorage) AddToFeed(title, author string, time time.Time) error {
	fmt.Printf("Adding to feed: %s by %s @%s\n", title, author, time)
	return nil
}

type FeedGenerator struct {
	feedStorage feedStorage
}

func (f FeedGenerator) UpdateFeed(message *message.Message) error {
	event := postAdded{}
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return err
	}

	err := f.feedStorage.AddToFeed(event.Title, event.Author, event.OccurredOn)
	if err != nil {
		return errors.Wrap(err, "cannot update feed")
	}

	return nil
}
