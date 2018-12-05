package main

import (
	"log"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
)

func main() {
	subscriber, err := kafka.NewConfluentSubscriber(
		kafka.SubscriberConfig{
			Brokers:       []string{"localhost:9092"},
			ConsumerGroup: "test_consumer_group",
		},
		kafka.DefaultMarshaler{},
		nil,
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe("example.topic")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publisher, err := kafka.NewPublisher([]string{"localhost:9092"}, kafka.DefaultMarshaler{}, nil)
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(uuid.NewV4().String(), []byte("Hello, world!"))

		if err := publisher.Publish("example.topic", msg); err != nil {
			panic(err)
		}
	}
}

func process(messages chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}
