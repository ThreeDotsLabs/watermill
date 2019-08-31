package pkg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Results struct {
	Count    int64
	Rate1    float64
	Rate5    float64
	Rate15   float64
	RateMean float64
}

// RunBenchmark runs benchmark on chosen pubsub and returns publishing and subscribing results.
func RunBenchmark(pubSubName string) (Results, Results, error) {
	if err := initialise(pubSubName); err != nil {
		return Results{}, Results{}, err
	}

	topic := "benchmark_" + watermill.NewShortUUID()

	pubsub, err := NewPubSub(pubSubName, topic)
	if err != nil {
		return Results{}, Results{}, err
	}

	if err := pubsub.PublishMessages(); err != nil {
		return Results{}, Results{}, err
	}

	m := metrics.NewMeter()

	go func() {
		for {
			fmt.Printf("processed: %d\n", m.Snapshot().Count())
			time.Sleep(time.Second * 5)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(pubsub.MessagesCount)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	go func() {
		err := pubsub.ConsumeMessages(router, &wg, m)
		if err != nil {
			panic(err)
		}
	}()

	wg.Wait()

	if err := pubsub.Close(); err != nil {
		return Results{}, Results{}, err
	}

	pubResults := Results{
		Count:    0,
		Rate1:    0,
		Rate5:    0,
		Rate15:   0,
		RateMean: 0,
	}

	ms := m.Snapshot()
	subResults := Results{
		Count:    ms.Count(),
		Rate1:    ms.Rate1(),
		Rate5:    ms.Rate5(),
		Rate15:   ms.Rate15(),
		RateMean: ms.RateMean(),
	}

	return pubResults, subResults, nil
}

// It is required to create a subscriber for some PubSubs for initialisation.
func initialise(pubSubName string) error {
	topic := "benchmark_init_" + watermill.NewShortUUID()

	pubsub, err := NewPubSub(pubSubName, topic)
	if err != nil {
		return err
	}

	if _, err := pubsub.Subscriber.Subscribe(context.Background(), topic); err != nil {
		return err
	}

	err = pubsub.Close()
	if err != nil {
		return err
	}

	return nil
}
