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
	"github.com/ThreeDotsLabs/watermill/components/requeuer"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func main() {
	db, err := stdSQL.Open("postgres", "postgres://watermill:password@localhost:5432/watermill?sslmode=disable")
	if err != nil {
		panic(err)
	}

	logger := watermill.NewStdLogger(false, false)

	poisonPublisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.ConditionalPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	if err != nil {
		panic(err)
	}

	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	if err != nil {
		panic(err)
	}

	poisonSubscriber, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		SchemaAdapter: sql.ConditionalPostgreSQLSchema{
			GenerateWhereClause: func(params sql.GenerateWhereClauseParams) (string, []any) {
				return "(metadata->>'delayed_until')::timestamptz < NOW() AT TIME ZONE 'UTC'", nil
			},
		},
		OffsetsAdapter: sql.ConditionalPostgreSQLOffsetsAdapter{
			DeleteOnAck: true,
		},
		InitializeSchema: true,
	}, logger)
	if err != nil {
		panic(err)
	}

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return params.EventName, nil
		},
		Marshaler: cqrs.JSONMarshaler{
			GenerateName: cqrs.EventName,
		},
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	poisonQueue, err := middleware.PoisonQueue(poisonPublisher, "poison")
	if err != nil {
		panic(err)
	}

	router := message.NewDefaultRouter(logger)
	router.AddMiddleware(poisonQueue)
	router.AddMiddleware(middleware.NewDelayMetadata(middleware.DelayMetadataConfig{}).Middleware)

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(router, cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return params.EventName, nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return sql.NewSubscriber(db, sql.SubscriberConfig{
				SchemaAdapter:    sql.DefaultPostgreSQLSchema{},
				OffsetsAdapter:   sql.DefaultPostgreSQLOffsetsAdapter{},
				InitializeSchema: true,
			}, logger)
		},
		Marshaler: cqrs.JSONMarshaler{
			GenerateName: cqrs.EventName,
		},
		Logger: logger,
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
				retries := msg.Metadata.Get(requeuer.RetriesKey)
				delayedUntil := msg.Metadata.Get(middleware.DelayedUntilKey)
				delayedFor := msg.Metadata.Get(middleware.DelayedForKey)

				if retries != "" {
					fmt.Println("\tRetries:", retries)
					fmt.Println("\tDelayed until:", delayedUntil)
					fmt.Println("\tDelayed for:", delayedFor)
				}

				if event.OrderID == "" {
					return fmt.Errorf("empty order_id")
				}

				return nil
			},
		),
	)
	if err != nil {
		panic(err)
	}

	requeuer, err := requeuer.NewRequeuer(requeuer.Config{
		Subscriber:     poisonSubscriber,
		SubscribeTopic: "poison",
		Publisher:      publisher,
		GeneratePublishTopic: func(params requeuer.GeneratePublishTopicParams) (string, error) {
			topic := params.Message.Metadata.Get(middleware.PoisonedTopicKey)
			if topic == "" {
				return "", fmt.Errorf("missing topic in metadata")
			}
			return topic, nil
		},
	}, logger)
	if err != nil {
		panic(err)
	}

	go func() {
		err = requeuer.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		for {
			var orderID string
			chance := rand.Intn(10)
			if chance > 2 {
				orderID = uuid.NewString()
			}
			e := OrderPlaced{
				OrderID: orderID,
			}
			err = eventBus.Publish(context.Background(), e)
			if err != nil {
				panic(err)
			}

			time.Sleep(1 * time.Second)
		}
	}()

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}

type OrderPlaced struct {
	OrderID string `json:"order_id"`
}

func (OrderPlaced) EventName() string {
	return "OrderPlaced"
}
