package main

import (
	"github.com/pkg/errors"
	"time"
	"github.com/roblaszczak/gooddd/domain/eventlistener"
	"fmt"
	"sync/atomic"
	"github.com/roblaszczak/gooddd/handler/plugin"
	"github.com/roblaszczak/gooddd/handler/middleware"

	"github.com/rcrowley/go-metrics"
	"github.com/roblaszczak/gooddd/handler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"log"
	"os"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"encoding/json"
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

func (p PostsCounter) Count(message handler.Message) ([]handler.MessagePayload, error) {
	newCount, err := p.countStorage.CountAdd()
	if err != nil {
		return nil, errors.Wrap(err, "cannot add count")
	}

	if newCount%100000 == 0 {
		fmt.Println("> new count:", newCount)
	}

	return []handler.MessagePayload{postsCountUpdated{newCount}}, nil
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

func (f FeedGenerator) UpdateFeed(message handler.Message) ([]handler.MessagePayload, error) {
	event := postAdded{}
	if err := handler.DecodeEventPayload(message, &event); err != nil {
		return nil, errors.Wrap(err, "cannot decode payload")
	}

	err := f.feedStorage.AddToFeed(event.Title, event.Author, event.OccurredOn)
	if err != nil {
		return nil, errors.Wrap(err, "cannot update feed")
	}

	return nil, nil
}

func LogEventMiddleware(h handler.Handler) handler.Handler {
	return func(event handler.Message) ([]handler.MessagePayload, error) {
		//fmt.Printf("event received: %#v\n", event)

		return h(event)
	}
}

func main() {
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

	// todo - move this boilerplate somewhere, to make examples more clear
	listenerFactory := eventlistener.NewConfluentKafkaFactory(func(kafkaMsg kafka.Message) (handler.Message, error) {
		msg := handler.Message{}
		if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
			return handler.Message{}, nil
		}

		return msg, nil
	}, func(subscriberMeta handler.SubscriberMetadata) string {
		return fmt.Sprintf("%s_%s", subscriberMeta.ServerName, subscriberMeta.SubscriberName)
	})

	router := handler.NewRouter("example", listenerFactory)

	metricsMiddleware := middleware.NewMetrics(t, errs, success)
	metricsMiddleware.ShowStats(time.Second*5, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	retryMiddleware := middleware.NewRetry()
	retryMiddleware.OnRetryHook = func(retryNum int, delay time.Duration) {
		//fmt.Println("retrying, num:", retryNum, "delay:", delay)
	}
	retryMiddleware.MaxRetries = 1
	retryMiddleware.WaitTime = time.Millisecond * 10

	router.AddMiddleware(
		metricsMiddleware.Middleware,
		middleware.PoisonQueueHook(func(message handler.Message) {
			fmt.Println("unable to process", message)
		}),
		retryMiddleware.Middleware,
		middleware.Recoverer,
		middleware.RandomFail(0.002),
		middleware.RandomPanic(0.002),
	)

	router.AddPlugin(plugin.SignalsHandler)

	router.Subscribe(
		"posts_counter",
		"test_topic",
		counter.Count,
	)
	router.Subscribe(
		"feed_generator",
		"test_topic",
		feedGenerator.UpdateFeed,
	)

	router.Run()
}
