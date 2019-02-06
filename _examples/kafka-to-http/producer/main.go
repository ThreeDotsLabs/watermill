package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
)

var (
	brokers = []string{"kafka:9092"}
	logger  = watermill.NewStdLogger(false, false)
)

type eventType string

const (
	Foo eventType = "Foo"
	Bar eventType = "Bar"
	Baz eventType = "Baz"
)

func main() {
	pub, err := kafka.NewPublisher(brokers, kafka.DefaultMarshaler{}, nil, logger)
	if err != nil {
		panic(err)
	}

	eventTypes := []eventType{Foo, Bar, Baz}

	for {
		eventType := eventTypes[rand.Intn(3)]
		msg := message.NewMessage(watermill.UUID(), []byte("message"))
		msg.Metadata.Set("event_type", string(eventType))

		fmt.Printf("%s Publishing %s\n\n", time.Now().String(), eventType)
		if err := pub.Publish("kafka_to_http_example", msg); err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}

}
