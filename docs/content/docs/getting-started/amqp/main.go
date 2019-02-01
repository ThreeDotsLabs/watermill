// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"log"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/amqp"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill/message"
)

var amqpURI = "amqp://guest:guest@rabbitmq:5672/"

func main() {
	amqpConfig := amqp.NewDurableQueueConfig(amqpURI)

	subscriber, err := amqp.NewSubscriber(
		// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-three-go.html
		// to create just simple queue, you can use NewDurableQueueConfig or create your own config
		amqpConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe("example.topic")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publisher, err := amqp.NewPublisher(amqpConfig, watermill.NewStdLogger(false, false))
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

		// we need to Acknowledge that we received and processed the message,
		// otherwise we will not receive next message
		msg.Ack()
	}
}
