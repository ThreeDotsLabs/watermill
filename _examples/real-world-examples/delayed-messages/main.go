package main

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	db, err := stdSQL.Open("postgres", "postgres://watermill:password@postgres:5432/watermill?sslmode=disable")
	if err != nil {
		panic(err)
	}

	logger := watermill.NewStdLogger(false, false)

	eventPublisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	if err != nil {
		panic(err)
	}

	commandPublisher, err := sql.NewDelayedPostgreSQLPublisher(db, sql.DelayedPostgreSQLPublisherConfig{
		DelayPublisherConfig: delay.PublisherConfig{},
		Logger:               logger,
	})
	if err != nil {
		panic(err)
	}

	eventBus, err := cqrs.NewEventBusWithConfig(eventPublisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return params.EventName, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	if err != nil {
		panic(err)
	}

	commandBus, err := cqrs.NewCommandBusWithConfig(commandPublisher, cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			return params.CommandName, nil
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

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(router, cqrs.CommandProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
			return params.CommandName, nil
		},
		SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return sql.NewDelayedPostgreSQLSubscriber(db, sql.DelayedPostgreSQLSubscriberConfig{
				DeleteOnAck: true,
				Logger:      logger,
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
				fmt.Printf("Received order placed from %v\n", event.Customer.Name)

				cmd := SendFeedbackForm{
					To:   event.Customer.Email,
					Name: event.Customer.Name,
				}

				// In a real world scenario, we would delay the command by a few days
				ctx = delay.WithContext(ctx, delay.For(10*time.Second))

				err := commandBus.Send(ctx, cmd)
				if err != nil {
					return err
				}

				return nil
			},
		),
	)
	if err != nil {
		panic(err)
	}

	err = commandProcessor.AddHandlers(
		cqrs.NewCommandHandler(
			"OnSendFeedbackForm",
			func(ctx context.Context, cmd *SendFeedbackForm) error {
				msg := fmt.Sprintf("Hello %s! It's been a while since you placed your order, how did you like it? Let us know!", cmd.Name)

				fmt.Println("Sending feedback form to:", cmd.To)
				fmt.Println("\tMessage:", msg)

				// In a real world scenario, we would send an email to the customer here

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

	for {
		e := OrderPlaced{
			OrderID: uuid.NewString(),
			Customer: Customer{
				Name:  gofakeit.FirstName(),
				Email: gofakeit.Email(),
			},
		}

		err = eventBus.Publish(context.Background(), e)
		if err != nil {
			panic(err)
		}

		time.Sleep(5 * time.Second)
	}
}

type Customer struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type OrderPlaced struct {
	OrderID  string   `json:"order_id"`
	Customer Customer `json:"customer"`
}

type SendFeedbackForm struct {
	To   string `json:"to"`
	Name string `json:"name"`
}
