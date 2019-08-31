package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
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

	messagePublished := make(chan struct{})
	allMessagesPublished := make(chan struct{})

	go func() {
		publishedMessages := 0
		for range messagePublished {
			publishedMessages++

			if publishedMessages%1000 == 0 {
				logger.Info("messages left", watermill.LogFields{"count": messagesToPublish - publishedMessages})
			}
			if publishedMessages >= messagesToPublish {
				allMessagesPublished <- struct{}{}
				break
			}
		}
	}()

	workerMessages := make(chan struct{})

	for num := 0; num < workers; num++ {
		go worker(publisher, workerMessages, messagePublished)
	}

	for i := 0; i < messagesToPublish; i++ {
		workerMessages <- struct{}{}
	}

	// Waiting for all messages to be published
	<-allMessagesPublished

	logger.Info("All messages published", watermill.LogFields{})
}

func worker(publisher message.Publisher, incomingMessage <-chan struct{}, messagePublished chan<- struct{}) {
	for range incomingMessage {
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

		messagePublished <- struct{}{}
	}
}

type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Content string `json:"content"`
}
