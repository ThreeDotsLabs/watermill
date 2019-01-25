package main

import (
	"log"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/amqp"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill/message"
)

var amqpURI = "amqp://guest:guest@rabbitmq:5672/"

func createSubscriber(queueSuffix string) *amqp.Subscriber {
	subscriber, err := amqp.NewSubscriber(
		amqp.NewDurablePubSubConfig(
			amqpURI,
			// queue name based on topic plus provided suffix,
			// exchange is fanout, so every subscriber with other suffix,
			// it will also receive all messages
			amqp.GenerateQueueNameTopicNameWithSuffix(queueSuffix),
		),
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}
	return subscriber
}

func main() {
	subscriber1 := createSubscriber("test_consumer_group_1")
	messages1, err := subscriber1.Subscribe("example.topic")
	if err != nil {
		panic(err)
	}
	go process("subscriber_1", messages1)

	subscriber2 := createSubscriber("test_consumer_group_2")
	messages2, err := subscriber2.Subscribe("example.topic")
	if err != nil {
		panic(err)
	}
	// subscriber2 will receive all messages independently from subscriber1
	go process("subscriber_2", messages2)

	publisher, err := amqp.NewPublisher(
		amqp.NewDurablePubSubConfig(
			amqpURI,
			nil, // generateQueueName is not used with publisher
		),
		watermill.NewStdLogger(false, false),
	)
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

func process(subscriber string, messages chan *message.Message) {
	for msg := range messages {
		log.Printf("[%s] received message: %s, payload: %s", subscriber, msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}
