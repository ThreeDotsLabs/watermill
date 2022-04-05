// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill-jetstream/pkg/jetstream"
	"github.com/nats-io/nats.go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	topic   = "example_topic"
	natsURL = "nats://nats-jetstream:4222"
)

func main() {
	marshaler := &jetstream.GobMarshaler{}
	logger := watermill.NewStdLogger(false, false)
	options := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.Timeout(30 * time.Second),
		nats.ReconnectWait(1 * time.Second),
	}
	subscribeOptions := []nats.SubOpt{
		nats.DeliverAll(),
		nats.AckExplicit(),
	}

	subscriber, err := jetstream.NewSubscriber(
		jetstream.SubscriberConfig{
			URL:              natsURL,
			CloseTimeout:     30 * time.Second,
			AckWaitTimeout:   30 * time.Second,
			NatsOptions:      options,
			Unmarshaler:      marshaler,
			SubscribeOptions: subscribeOptions,
			AutoProvision:    true,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		panic(err)
	}

	go process(messages)

	publisher, err := jetstream.NewPublisher(
		jetstream.PublisherConfig{
			URL:         natsURL,
			NatsOptions: options,
			Marshaler:   marshaler,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))

		if err := publisher.Publish(topic, msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
