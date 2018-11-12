package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka/marshal"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/renstrom/shortuuid"
)

type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Content string `json:"content"`
}

var letters = []rune("abcdefghijklmnopqrstuvwxyz")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	publisher, err := kafka.NewPublisher([]string{"localhost:9092"}, marshal.ConfluentKafka{}, nil)
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	messagesToAdd := 1000

	msgAdded := make(chan struct{})
	allMessagesAdded := make(chan struct{})

	go func() {
		for range msgAdded {
			messagesToAdd--

			if messagesToAdd%100000 == 0 {
				fmt.Println("left ", messagesToAdd)
			}
			if messagesToAdd == 0 {
				allMessagesAdded <- struct{}{}
			}
		}
	}()

	for num := 0; num < 25; num++ {
		go func() {
			var msgPayload postAdded
			var msg *message.Message

			for messagesToAdd > 0 {
				msgPayload.OccurredOn = time.Now()
				msgPayload.Author = randString(10)
				msgPayload.Title = randString(15)
				msgPayload.Content = randString(30)

				b, err := json.Marshal(msgPayload)
				if err != nil {
					panic(err)
				}
				msg = message.NewMessage(uuid.NewV4().String(), b)

				middleware.SetCorrelationID(shortuuid.New(), msg)

				err = publisher.Publish("test_topic", msg)
				if err != nil {
					log.Println("cannot publish message:", err)
					continue
				}
				msgAdded <- struct{}{}
			}
		}()
	}

	<-allMessagesAdded
}
