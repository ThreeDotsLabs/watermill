// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"log"
	"time"

	"github.com/nats-io/go-nats-streaming"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	subscriber, err := nats.NewStreamingSubscriber(
		nats.StreamingSubscriberConfig{
			ClusterID:        "test-cluster",
			ClientID:         "example-subscriber",
			QueueGroup:       "example",
			DurableName:      "my-durable",
			SubscribersCount: 4, // how much goroutines should consume messages
			CloseTimeout:     time.Minute,
			AckWaitTimeout:   time.Second * 30,
			StanOptions: []stan.Option{
				stan.NatsURL("nats://nats-streaming:4222"),
			},
			Unmarshaler: nats.GobMarshaler{},
		},
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

	publisher, err := nats.NewStreamingPublisher(
		nats.StreamingPublisherConfig{
			ClusterID: "test-cluster",
			ClientID:  "example-publisher",
			StanOptions: []stan.Option{
				stan.NatsURL("nats://nats-streaming:4222"),
			},
			Marshaler: nats.GobMarshaler{},
		},
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

func process(messages chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise we will not receive next message
		msg.Ack()
	}
}
