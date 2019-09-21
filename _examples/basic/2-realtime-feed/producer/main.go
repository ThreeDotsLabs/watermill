package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

var (
	brokers = []string{"kafka:9092"}

	messagesPerSecond = 100
	numWorkers        = 20
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	logger.Info("Starting the producer", watermill.LogFields{})

	rand.Seed(time.Now().Unix())

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	closeCh := make(chan struct{})
	workersGroup := &sync.WaitGroup{}
	workersGroup.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go worker(publisher, workersGroup, closeCh)
	}

	// wait for SIGINT
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// signal for the workers to stop publishing
	close(closeCh)

	// Waiting for all messages to be published
	workersGroup.Wait()

	logger.Info("All messages published", nil)
}

// worker publishes messages until closeCh is closed.
func worker(publisher message.Publisher, wg *sync.WaitGroup, closeCh chan struct{}) {
	ticker := time.NewTicker(time.Duration(int(time.Second) / messagesPerSecond))

	for {
		select {
		case <-closeCh:
			ticker.Stop()
			wg.Done()
			return

		case <-ticker.C:
		}

		msgPayload := postAdded{
			OccurredOn: time.Now(),
			Author:     gofakeit.Username(),
			Title:      gofakeit.Sentence(rand.Intn(5) + 1),
			Content:    gofakeit.Sentence(rand.Intn(10) + 5),
		}

		payload, err := json.Marshal(msgPayload)
		if err != nil {
			panic(err)
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)

		// Use a middleware to set the correlation ID, it's useful for debugging
		middleware.SetCorrelationID(watermill.NewShortUUID(), msg)
		err = publisher.Publish("posts_published", msg)
		if err != nil {
			fmt.Println("cannot publish message:", err)
			continue
		}
	}
}

type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Content string `json:"content"`
}
