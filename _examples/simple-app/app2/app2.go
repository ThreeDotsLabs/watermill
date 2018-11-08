package main

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message/router/middleware"
	"github.com/roblaszczak/gooddd/message/router/plugin"

	"log"
	"net/http"
	"os"

	"github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
	"github.com/roblaszczak/gooddd/message"

	_ "net/http/pprof"

	"github.com/roblaszczak/gooddd"
	kafka2 "github.com/roblaszczak/gooddd/message/infrastructure/kafka"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/satori/go.uuid"
)

// todo - doc why separated type
type postAdded struct {
	Author     string    `json:"author"`
	Title      string    `json:"title"`
	OccurredOn time.Time `json:"occurred_on"`
}

type postsCountUpdated struct {
	NewCount int64 `json:"new_count"`
}

// todo - replace with redis?
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
	newCount, err := p.countStorage.CountAdd()
	if err != nil {
		return nil, errors.Wrap(err, "cannot add count")
	}

	if newCount%100000 == 0 {
		fmt.Println("> new count:", newCount)
	}

	producedMsg := postsCountUpdated{NewCount: newCount}
	b, _ := json.Marshal(producedMsg)
	//producedMsg.

	return []*message.Message{message.NewMessage(uuid.NewV4().String(), b)}, nil
}

// todo - replace with mongo?
type feedStorage interface {
	AddToFeed(title, author string, time time.Time) error
}

// todo - doc (stub)
type printFeedStorage struct{}

func (printFeedStorage) AddToFeed(title, author string, time time.Time) error {
	//fmt.Printf("Adding to feed: %s by %s @%s\n", title, author, time)
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

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	logger := gooddd.NewStdLogger(true, true)
	//logger := gooddd.NopLogger{}

	t := metrics.NewTimer()
	metrics.Register("handler.time", t)

	errs := metrics.NewCounter()
	metrics.Register("handler.errors", errs)

	success := metrics.NewCounter()
	metrics.Register("handler.success", success)

	// todo - use not default registry
	// todo - rewrite
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

	counter := PostsCounter{memoryCountStorage{new(int64)}}
	feedGenerator := FeedGenerator{printFeedStorage{}}

	marshaler := marshal.ConfluentKafka{}
	brokers := []string{"localhost:9092"}
	pub, err := kafka2.NewPublisher(brokers, marshaler)
	if err != nil {
		panic(err)
	}

	sub, err := kafka2.NewConfluentSubscriber(
		kafka2.SubscriberConfig{
			Brokers:        brokers,
			ConsumersCount: 8,
		},
		marshaler,
		logger,
	)
	if err != nil {
		panic(err)
	}

	pubSub := message.NewPubSub(pub, sub)

	h, err := message.NewRouter(
		message.RouterConfig{
			ServerName:         "example_v2",
			PublishEventsTopic: "app2_events",
		},
		pubSub,
		pubSub,
	)
	if err != nil {
		panic(err)
	}
	h.Logger = logger

	metricsMiddleware := middleware.NewMetrics(t, errs, success)
	metricsMiddleware.ShowStats(time.Second*5, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	retryMiddleware := middleware.NewRetry()
	retryMiddleware.OnRetryHook = func(retryNum int, delay time.Duration) {
		// you can use dead letter queue here, for example
		fmt.Println("retrying, num:", retryNum, "delay:", delay)
	}
	retryMiddleware.MaxRetries = 1
	retryMiddleware.WaitTime = time.Millisecond * 10

	throttle, err := middleware.NewThrottlePerSecond(1, logger)
	if err != nil {
		panic(err)
	}

	h.AddMiddleware(
		metricsMiddleware.Middleware,
		middleware.AckOnSuccess,
		throttle.Middleware,
		//middleware.PoisonQueueHook(func(message *message.Message, err error) {
		//	fmt.Println("unable to process", message, "err:", err)
		//}),
		retryMiddleware.Middleware,
		middleware.Recoverer,
		middleware.CorrelationID,
		middleware.RandomFail(0.002),
		middleware.RandomPanic(0.002),
	)

	h.AddPlugin(plugin.SignalsHandler)

	h.AddHandler(
		"posts_counter",
		"test_topic",
		counter.Count,
	)
	h.AddHandler(
		"feed_generator",
		"test_topic",
		feedGenerator.UpdateFeed,
	)

	h.Run()
}
