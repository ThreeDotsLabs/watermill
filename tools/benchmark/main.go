package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/nats"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
)

var pubsubFlag = flag.String("pubsub", "", "")

var logger = watermill.NopLogger{}

const defaultMessagesCount = 1000000

var topic = "benchmark_" + watermill.NewShortUUID()

type pubSub struct {
	Constructor              func() message.PubSub
	RequireConcurrentProduce bool
	MessagesCount            int
}

var pubSubs = map[string]pubSub{
	"gochannel": {
		Constructor: func() message.PubSub {
			return gochannel.NewGoChannel(gochannel.Config{}, logger)
		},
		RequireConcurrentProduce: true,
	},
	"kafka": {
		Constructor: func() message.PubSub {
			publisher, err := kafka.NewPublisher(
				[]string{"localhost:9092"},
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
					Brokers:       []string{"localhost:9092"},
					ConsumerGroup: "benchmark",
				},
				saramaConfig,
				kafka.DefaultMarshaler{},
				logger,
			)
			if err != nil {
				panic(err)
			}

			return message.NewPubSub(publisher, subscriber)
		},
	},
	"nats": {
		Constructor: func() message.PubSub {
			pub, err := nats.NewStreamingPublisher(nats.StreamingPublisherConfig{
				ClusterID: "test-cluster",
				ClientID:  "benchmark_pub",
				Marshaler: nats.GobMarshaler{},
			}, logger)
			if err != nil {
				panic(err)
			}

			sub, err := nats.NewStreamingSubscriber(nats.StreamingSubscriberConfig{
				ClusterID:        "test-cluster",
				ClientID:         "benchmark_sub",
				QueueGroup:       "test-queue",
				DurableName:      "durable-name",
				SubscribersCount: 8, // todo - experiment
				Unmarshaler:      nats.GobMarshaler{},
				AckWaitTimeout:   time.Second,
			}, logger)
			if err != nil {
				panic(err)
			}

			return message.NewPubSub(pub, sub)
		},
	},
	"gcloud": {
		Constructor: func() message.PubSub {
			ctx := context.Background()

			// todo - doc hostname
			publisher, err := googlecloud.NewPublisher(
				ctx,
				googlecloud.PublisherConfig{
					ProjectID: os.Getenv("GOOGLE_CLOUD_PROJECT"),
					Marshaler: googlecloud.DefaultMarshalerUnmarshaler{},
				},
			)
			if err != nil {
				panic(err)
			}

			sub := &subscriber.Multiplier{
				Constructor: func() message.Subscriber {
					subscriber, err := googlecloud.NewSubscriber(
						ctx,
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
						panic(err)
					}

					return subscriber
				},
				Count: 100,
			}

			return message.NewPubSub(publisher, sub)
		},
	},
}

func main() {
	flag.Parse()

	fmt.Printf("starting benchmark, topic %s, pubsub: %s\n", topic, *pubsubFlag)

	pubsub := pubSubs[*pubsubFlag]
	if pubsub.MessagesCount == 0 {
		pubsub.MessagesCount = defaultMessagesCount
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	allPublished := make(chan struct{})

	var m metrics.Meter

	// it is required to create sub before for some pubsubs
	ps := pubsub.Constructor()

	if _, err := ps.Subscribe(context.Background(), topic); err != nil {
		panic(err)
	}
	if err := ps.Close(); err != nil {
		panic(err)
	}

	publishMessages(pubsub)
	close(allPublished)

	m = metrics.NewMeter()

	go func() {
		for {
			fmt.Printf("processed: %d\n", m.Snapshot().Count())
			time.Sleep(time.Second * 5)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(pubsub.MessagesCount)

	go func() {
		router.AddNoPublisherHandler(
			"benchmark_read",
			topic,
			pubsub.Constructor(),
			func(msg *message.Message) (messages []*message.Message, e error) {
				defer wg.Done()
				defer m.Mark(1)

				msg.Ack()
				return nil, nil
			},
		)

		if err := router.Run(); err != nil {
			panic(err)
		}
	}()

	wg.Wait()

	ms := m.Snapshot()
	fmt.Printf("  count:       %9d\n", ms.Count())
	fmt.Printf("  1-min rate:  %12.2f\n", ms.Rate1())
	fmt.Printf("  5-min rate:  %12.2f\n", ms.Rate5())
	fmt.Printf("  15-min rate: %12.2f\n", ms.Rate15())
	fmt.Printf("  mean rate:   %12.2f\n", ms.RateMean())
}

func publishMessages(ps pubSub) {
	publisher := ps.Constructor()

	messagesLeft := ps.MessagesCount
	workers := 200

	addMsg := make(chan struct{})
	wg := sync.WaitGroup{}

	start := time.Now()

	for num := 0; num < workers; num++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			msgPayload := []byte("foo bar baz")
			var msg *message.Message

			for range addMsg {
				msg = message.NewMessage(watermill.NewULID(), msgPayload)

				// using function from middleware to set correlation id, useful for debugging
				middleware.SetCorrelationID(watermill.NewShortUUID(), msg)

				if err := publisher.Publish(topic, msg); err != nil {
					panic(err)
				}
			}
		}()
	}

	for ; messagesLeft > 0; messagesLeft-- {
		addMsg <- struct{}{}
	}
	close(addMsg)

	wg.Wait()

	if err := publisher.Close(); err != nil {
		panic(err)
	}

	elapsed := time.Now().Sub(start)
	fmt.Printf("added %d messages in %s, %f msg/s\n", ps.MessagesCount, elapsed, float64(ps.MessagesCount)/elapsed.Seconds())
}
