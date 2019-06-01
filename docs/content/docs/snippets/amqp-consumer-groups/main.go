package main

import (
	"context"
	"log"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/amqp"
)

var amqpURI = "amqp://guest:guest@rabbitmq:5672/"

func createSubscriber(queueSuffix string) *amqp.Subscriber {
	// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-three-go.html
	// to create just a simple queue, you can use NewDurableQueueConfig or create your own config.
	config := amqp.NewDurablePubSubConfig(
		amqpURI,
		// Rabbit's queue name in this example is based on Watermill's topic passed to Subscribe
		// plus provided suffix.
		//
		// Exchange is Rabbit's "fanout", so when subscribing with suffix other than "test_consumer_group",
		// it will also receive all messages. It will work like separate consumer groups in Kafka.
		amqp.GenerateQueueNameTopicNameWithSuffix(queueSuffix),
	)
	config.Logger = watermill.NewStdLogger(false, false)

	subscriber, err := amqp.NewSubscriber(config)
	if err != nil {
		panic(err)
	}
	return subscriber
}

func main() {
	subscriber1 := createSubscriber("test_consumer_group_1")
	messages1, err := subscriber1.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}
	go process("subscriber_1", messages1)

	subscriber2 := createSubscriber("test_consumer_group_2")
	messages2, err := subscriber2.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}
	// subscriber2 will receive all messages independently from subscriber1
	go process("subscriber_2", messages2)

	config := amqp.NewDurablePubSubConfig(
		amqpURI,
		nil, // generateQueueName is not used with publisher
	)
	config.Logger = watermill.NewStdLogger(false, false)

	publisher, err := amqp.NewPublisher(config)
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))

		if err := publisher.Publish("example.topic", msg); err != nil {
			panic(err)
		}
	}
}

func process(subscriber string, messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("[%s] received message: %s, payload: %s", subscriber, msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}
