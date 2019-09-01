package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

var (
	brokers = []string{"kafka:9092"}

	messagesToPublish = 10000
	workers           = 25
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	logger.Info("Starting the producer", watermill.LogFields{})

	rand.Seed(time.Now().Unix())

	publisher, err := kafka.NewPublisher(brokers, kafka.DefaultMarshaler{}, nil, logger)
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	messageStream := make(chan *message.Message, workers)
	workersGroup := &sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		go worker(publisher, workersGroup, messageStream)
	}

	for i := 0; i < messagesToPublish; i++ {
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

		messageStream <- msg
	}
	close(messageStream)

	// Waiting for all messages to be published
	workersGroup.Wait()

	logger.Info("All messages published", watermill.LogFields{})
}

func worker(publisher message.Publisher, wg *sync.WaitGroup, messages <-chan *message.Message) {
	wg.Add(1)
	for msg := range messages {
		err := publisher.Publish("posts_published", msg)
		if err != nil {
			fmt.Println("cannot publish message:", err)
			continue
		}
	}
	wg.Done()
}

type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Content string `json:"content"`
}
