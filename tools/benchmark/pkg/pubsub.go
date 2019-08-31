package pkg

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/nats-io/stan.go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill-kafka/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

const (
	defaultMessagesCount = 1000000
	defaultMessageSize   = 1024
)

var logger = watermill.NopLogger{}

type PubSub struct {
	Publisher  message.Publisher
	Subscriber message.Subscriber

	MessagesCount int
	MessageSize   int

	Topic string
}

func NewPubSub(name string, topic string) (PubSub, error) {
	definition, ok := pubSubDefinitions[name]
	if !ok {
		return PubSub{}, fmt.Errorf("unknown PubSub: %s", name)
	}

	pub, sub := definition.Constructor()

	return PubSub{
		Publisher:  pub,
		Subscriber: sub,

		MessagesCount: defaultMessagesCount,
		MessageSize:   defaultMessageSize,
		Topic:         topic,
	}, nil
}

func (ps PubSub) Close() error {
	if err := ps.Publisher.Close(); err != nil {
		return err
	}
	return ps.Subscriber.Close()
}

type PubSubDefinition struct {
	Constructor func() (message.Publisher, message.Subscriber)
}

var pubSubDefinitions = map[string]PubSubDefinition{
	"gochannel": {
		Constructor: func() (message.Publisher, message.Subscriber) {
			pubsub := gochannel.NewGoChannel(gochannel.Config{}, logger)
			return pubsub, pubsub
		},
	},
	"kafka": {
		Constructor: func() (message.Publisher, message.Subscriber) {
			broker := os.Getenv("WATERMILL_KAFKA_BROKER")
			if broker == "" {
				broker = "kafka:9092"
			}

			publisher, err := kafka.NewPublisher(
				[]string{broker},
				kafka.DefaultMarshaler{},
				nil,
				logger,
			)
			if err != nil {
				panic(err)
			}

			saramaConfig := kafka.DefaultSaramaSubscriberConfig()
			saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

			subscriber, err := kafka.NewSubscriber(
				kafka.SubscriberConfig{
					Brokers:       []string{broker},
					ConsumerGroup: "benchmark",
				},
				saramaConfig,
				kafka.DefaultMarshaler{},
				logger,
			)
			if err != nil {
				panic(err)
			}

			return publisher, subscriber
		},
	},
	"nats": {
		Constructor: func() (message.Publisher, message.Subscriber) {
			natsURL := os.Getenv("WATERMILL_NATS_URL")
			if natsURL == "" {
				natsURL = "nats://nats-streaming:4222"
			}

			pub, err := nats.NewStreamingPublisher(nats.StreamingPublisherConfig{
				ClusterID: "test-cluster",
				ClientID:  "benchmark_pub_" + watermill.NewShortUUID(),
				StanOptions: []stan.Option{
					stan.NatsURL(natsURL),
				},
				Marshaler: nats.GobMarshaler{},
			}, logger)
			if err != nil {
				panic(err)
			}

			sub, err := nats.NewStreamingSubscriber(nats.StreamingSubscriberConfig{
				ClusterID:        "test-cluster",
				ClientID:         "benchmark_sub_" + watermill.NewShortUUID(),
				QueueGroup:       "test-queue",
				DurableName:      "durable-name",
				SubscribersCount: 8, // todo - experiment
				Unmarshaler:      nats.GobMarshaler{},
				AckWaitTimeout:   time.Second,
				StanOptions: []stan.Option{
					stan.NatsURL(natsURL),
				},
			}, logger)
			if err != nil {
				panic(err)
			}

			return pub, sub
		},
	},
	"googlecloud": {
		Constructor: func() (message.Publisher, message.Subscriber) {
			// todo - doc hostname
			pub, err := googlecloud.NewPublisher(
				googlecloud.PublisherConfig{
					ProjectID: os.Getenv("GOOGLE_CLOUD_PROJECT"),
					Marshaler: googlecloud.DefaultMarshalerUnmarshaler{},
				}, logger,
			)
			if err != nil {
				panic(err)
			}

			sub := NewMultiplier(
				func() (message.Subscriber, error) {
					subscriber, err := googlecloud.NewSubscriber(
						googlecloud.SubscriberConfig{
							ProjectID: os.Getenv("GOOGLE_CLOUD_PROJECT"),
							GenerateSubscriptionName: func(topic string) string {
								return topic
							},
							Unmarshaler: googlecloud.DefaultMarshalerUnmarshaler{},
						},
						logger,
					)
					if err != nil {
						return nil, err
					}

					return subscriber, nil
				}, 100,
			)

			return pub, sub
		},
	},
}
