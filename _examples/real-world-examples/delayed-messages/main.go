package main

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	db, err := stdSQL.Open("postgres", "postgres://watermill:password@localhost:5432/watermill?sslmode=disable")
	if err != nil {
		panic(err)
	}

	logger := watermill.NewStdLogger(false, false)

	publisher, err := sql.NewDelayedPostgresPublisher(db, sql.DelayedPostgresPublisherConfig{
		DelayPublisherConfig: delay.PublisherConfig{
			DefaultDelay: delay.For(10 * time.Second),
		},
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return params.EventName, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	if err != nil {
		panic(err)
	}

	router := message.NewDefaultRouter(logger)

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(router, cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return params.EventName, nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return sql.NewDelayedPostgresSubscriber(db, sql.DelayedPostgresSubscriberConfig{
				Logger: logger,
			})
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	if err != nil {
		panic(err)
	}

	err = eventProcessor.AddHandlers(
		cqrs.NewEventHandler(
			"OnOrderPlacedHandler",
			func(ctx context.Context, event *OrderPlaced) error {
				fmt.Println("Received order placed:", event.OrderID)

				msg := cqrs.OriginalMessageFromCtx(ctx)
				delayedUntil := msg.Metadata.Get(delay.DelayedUntilKey)
				delayedFor := msg.Metadata.Get(delay.DelayedForKey)

				if delayedUntil != "" {
					fmt.Println("\tDelayed until:", delayedUntil)
					fmt.Println("\tDelayed for:", delayedFor)
				}

				return nil
			},
		),
	)
	if err != nil {
		panic(err)
	}

	go func() {
		err = router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-router.Running()

	msg := message.NewMessage(watermill.NewUUID(), nil)
	delay.Message(msg, delay.For(10*time.Second))

	for {
		e := OrderPlaced{
			OrderID: uuid.NewString(),
		}

		ctx := context.Background()

		chance := rand.Intn(10)
		if chance > 8 {
			ctx = delay.WithContext(ctx, delay.Until(time.Now().UTC().Add(time.Minute)))
		} else if chance > 5 {
			ctx = delay.WithContext(ctx, delay.For(20*time.Second))
		}

		err = eventBus.Publish(ctx, e)
		if err != nil {
			panic(err)
		}

		time.Sleep(1 * time.Second)
	}
}

type OrderPlaced struct {
	OrderID string `json:"order_id"`
}
