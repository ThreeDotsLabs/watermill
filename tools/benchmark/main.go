package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill-kafka/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var pubsubFlag = flag.String("pubsub", "", "")

var logger = watermill.NopLogger{}

const defaultMessagesCount = 1000000

var topic = "benchmark_" + watermill.NewShortUUID()

type pubSub struct {
	Constructor              func() (message.Publisher, message.Subscriber)
	RequireConcurrentProduce bool
	MessagesCount            int
}

var pubSubs = map[string]pubSub{
	"gochannel": {
		Constructor: func() (message.Publisher, message.Subscriber) {
			pubsub := gochannel.NewGoChannel(gochannel.Config{}, logger)
			return pubsub, pubsub
		},
		RequireConcurrentProduce: true,
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

			return pub, sub
		},
	},
	"gcloud": {
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
	pub, sub := pubsub.Constructor()

	if _, err := sub.Subscribe(context.Background(), topic); err != nil {
		panic(err)
	}
	if err := pub.Close(); err != nil {
		panic(err)
	}
	if err := sub.Close(); err != nil {
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
		_, sub := pubsub.Constructor()
		router.AddNoPublisherHandler(
			"benchmark_read",
			topic,
			sub,
			func(msg *message.Message) error {
				defer wg.Done()
				defer m.Mark(1)

				msg.Ack()
				return nil
			},
		)

		if err := router.Run(context.Background()); err != nil {
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
	pub, _ := ps.Constructor()

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

				if err := pub.Publish(topic, msg); err != nil {
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

	if err := pub.Close(); err != nil {
		panic(err)
	}

	elapsed := time.Now().Sub(start)
	fmt.Printf("added %d messages in %s, %f msg/s\n", ps.MessagesCount, elapsed, float64(ps.MessagesCount)/elapsed.Seconds())
}

type Constructor func() (message.Subscriber, error)

type multiplier struct {
	subscriberConstructor func() (message.Subscriber, error)
	subscribersCount      int
	subscribers           []message.Subscriber
}

func NewMultiplier(constructor Constructor, subscribersCount int) message.Subscriber {
	return &multiplier{
		subscriberConstructor: constructor,
		subscribersCount:      subscribersCount,
	}
}

func (s *multiplier) Subscribe(ctx context.Context, topic string) (msgs <-chan *message.Message, err error) {
	defer func() {
		if err != nil {
			if closeErr := s.Close(); closeErr != nil {
				err = multierror.Append(err, closeErr)
			}
		}
	}()

	out := make(chan *message.Message)

	subWg := sync.WaitGroup{}
	subWg.Add(s.subscribersCount)

	for i := 0; i < s.subscribersCount; i++ {
		sub, err := s.subscriberConstructor()
		if err != nil {
			return nil, errors.Wrap(err, "cannot create subscriber")
		}

		s.subscribers = append(s.subscribers, sub)

		msgs, err := sub.Subscribe(ctx, topic)
		if err != nil {
			return nil, errors.Wrap(err, "cannot subscribe")
		}

		go func() {
			for msg := range msgs {
				out <- msg
			}
			subWg.Done()
		}()
	}

	go func() {
		subWg.Wait()
		close(out)
	}()

	return out, nil
}

func (s *multiplier) Close() error {
	var err error

	for _, sub := range s.subscribers {
		if closeErr := sub.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}

	return err
}
