package main

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/lib/pq"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/delay"
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

	poisonPublisher, err := sql.NewDelayedPostgreSQLPublisher(db, sql.DelayedPostgreSQLPublisherConfig{
		Logger: logger,
	})
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

	poisonSubscriber, err := sql.NewDelayedPostgreSQLSubscriber(db, sql.DelayedPostgreSQLSubscriberConfig{
		DeleteOnAck: true,
		Logger:      logger,
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

	poisonQueue, err := middleware.PoisonQueue(poisonPublisher, "poison")
	if err != nil {
		panic(err)
	}

	router := message.NewDefaultRouter(logger)
	router.AddMiddleware(poisonQueue)
	router.AddMiddleware(middleware.NewDelayOnError(middleware.DelayOnErrorConfig{}).Middleware)

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
				retries := msg.Metadata.Get(requeuer.RetriesKey)
				delayedUntil := msg.Metadata.Get(delay.DelayedUntilKey)
				delayedFor := msg.Metadata.Get(delay.DelayedForKey)

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
		err = router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-router.Running()

	for {
		e := newFakeOrderPlaced()

		chance := rand.Intn(10)
		if chance < 2 {
			e.OrderID = ""
		}

		err = eventBus.Publish(context.Background(), e)
		if err != nil {
			panic(err)
		}

		time.Sleep(1 * time.Second)
	}
}

func newFakeOrderPlaced() OrderPlaced {
	var products []Product

	for i := 0; i < rand.Intn(5)+1; i++ {
		products = append(products, Product{
			ID:   watermill.NewShortUUID(),
			Name: gofakeit.ProductName(),
		})
	}

	return OrderPlaced{
		OrderID: watermill.NewUUID(),
		Customer: Customer{
			ID:    watermill.NewULID(),
			Name:  gofakeit.Name(),
			Email: gofakeit.Email(),
			Phone: gofakeit.Phone(),
		},
		Address: Address{
			Street:  gofakeit.Street(),
			City:    gofakeit.City(),
			Zip:     gofakeit.Zip(),
			Country: gofakeit.Country(),
		},
		Products: products,
	}
}

type OrderPlaced struct {
	OrderID  string    `json:"order_id"`
	Customer Customer  `json:"customer"`
	Address  Address   `json:"address"`
	Products []Product `json:"products"`
}

type Customer struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
	Phone string `json:"phone"`
}

type Address struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	Zip     string `json:"zip"`
	Country string `json:"country"`
}

type Product struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
