package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metrics "github.com/rcrowley/go-metrics"
	uuid "github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	marshaler = kafka.DefaultMarshaler{}
	brokers   = []string{"kafka:9092"}

	logger = watermill.NewStdLogger(false, false)
)

func main() {
	pub, err := kafka.NewPublisher(brokers, marshaler, nil, logger)
	if err != nil {
		panic(err)
	}

	r, err := message.NewRouter(
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

	r.AddMiddleware(
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
	r.AddPlugin(plugin.SignalsHandler)

	// handler which just counts added posts
	if err = r.AddHandler(
		"posts_counter",
		"posts_published",
		"posts_count",
		message.NewPubSub(pub, createSubscriber("posts_counter_v2", logger)),
		PostsCounter{memoryCountStorage{new(int64)}}.Count,
	); err != nil {
		panic(err)
	}

	// handler which generates "feed" from events post
	//
	// this implementation just prints it to stdout,
	// but production ready implementation would save posts to some persistent storage
	if err = r.AddNoPublisherHandler(
		"feed_generator",
		"posts_published",
		createSubscriber("feed_generator_v2", logger),
		FeedGenerator{printFeedStorage{}}.UpdateFeed,
	); err != nil {
		panic(err)
	}

	if err = r.Run(); err != nil {
		panic(err)
	}

}

func createSubscriber(consumerGroup string, logger watermill.LoggerAdapter) message.Subscriber {
	sub, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       brokers,
			ConsumerGroup: consumerGroup,
		},
		nil,
		marshaler,
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
	// in production use when implementing counter we probably want to make some kind of deduplication here

	newCount, err := p.countStorage.CountAdd()
	if err != nil {
		return nil, errors.Wrap(err, "cannot add count")
	}

	producedMsg := postsCountUpdated{NewCount: newCount}
	b, err := json.Marshal(producedMsg)
	if err != nil {
		return nil, err
	}

	return []*message.Message{message.NewMessage(uuid.NewV4().String(), b)}, nil
}

// intentionally not importing type from app1, because we don't need all data and we want to avoid coupling
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

func (f FeedGenerator) UpdateFeed(message *message.Message) ([]*message.Message, error) {
	event := postAdded{}
	json.Unmarshal(message.Payload, &event)

	err := f.feedStorage.AddToFeed(event.Title, event.Author, event.OccurredOn)
	if err != nil {
		return nil, errors.Wrap(err, "cannot update feed")
	}

	return nil, nil
}

func newMetricsMiddleware() middleware.Metrics {
	t := metrics.NewTimer()
	metrics.Register("handler.time", t)

	errs := metrics.NewCounter()
	metrics.Register("handler.errors", errs)

	success := metrics.NewCounter()
	metrics.Register("handler.success", success)

	pClient := prometheusmetrics.NewPrometheusProvider(
		metrics.DefaultRegistry,
		"test",
		"subsys",
		prometheus.DefaultRegisterer,
		1*time.Second,
	)
	go pClient.UpdatePrometheusMetrics()
	http.Handle("/metrics", promhttp.Handler())

	go http.ListenAndServe(":9000", nil)
	metricsMiddleware := middleware.NewMetrics(t, errs, success)
	metricsMiddleware.ShowStats(time.Second*5, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	return metricsMiddleware
}
