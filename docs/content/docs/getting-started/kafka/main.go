// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"
	"time"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
)

func main() {
	saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
	// equivalent of auto.offset.reset: earliest
	saramaSubscriberConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       []string{"kafka:9092"},
			ConsumerGroup: "test_consumer_group",
		},
		saramaSubscriberConfig,
		kafka.DefaultMarshaler{},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publisher, err := kafka.NewPublisher(
		[]string{"kafka:9092"},
		kafka.DefaultMarshaler{},
		nil, // no custom sarama config
		watermill.NewStdLogger(false, false),
	)
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
