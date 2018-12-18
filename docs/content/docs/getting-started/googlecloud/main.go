// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	subscriber, err := googlecloud.NewSubscriber(
		context.Background(),
		googlecloud.SubscriberConfig{
			// custom function to generate Subscription Name,
			// there are also predefined TopicSubscriptionName and TopicSubscriptionNameWithSuffix available.
			GenerateSubscriptionName: func(topic string) string {
				return "test-sub_" + topic
			},
			ProjectID: "test-project",
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	// Subscribe will create the subscription. Only messages that are sent after the subscription is created may be received.
	messages, err := subscriber.Subscribe("example.topic")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publisher, err := googlecloud.NewPublisher(context.Background(), googlecloud.PublisherConfig{
		ProjectID: "test-project",
	})
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
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
