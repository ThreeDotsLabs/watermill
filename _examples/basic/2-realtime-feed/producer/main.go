package main

import (
	"encoding/json"
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

	publisher, err := kafka.NewPublisher(brokers, kafka.DefaultMarshaler{}, nil, logger)
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	msgPublished := make(chan struct{})
	allMessagesPublished := make(chan struct{})

	go func() {
		for range msgPublished {
			messagesToPublish--

			if messagesToPublish%1000 == 0 {
				logger.Info("messages left", watermill.LogFields{"count": messagesToPublish})
			}
			if messagesToPublish == 0 {
				allMessagesPublished <- struct{}{}
			}
		}
	}()

	for num := 0; num < workers; num++ {
		go func() {
			for messagesToPublish > 0 {
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
					logger.Error("cannot publish message:", err, watermill.LogFields{})
					continue
				}
				msgPublished <- struct{}{}
			}
		}()
	}

	// Waiting for all messages to be published
	<-allMessagesPublished

	logger.Info("All messages published", watermill.LogFields{})
}

type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Content string `json:"content"`
}
