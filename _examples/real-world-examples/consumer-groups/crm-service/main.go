package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill-routing-example/server/common"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/redis/go-redis/v9"
)

var (
	replica                = os.Getenv("REPLICA")
	serviceName            = "crm-service"
	serviceNameWithReplica = serviceName + "-" + replica
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	pubClient := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	publisher, err := redisstream.NewPublisher(
		redisstream.PublisherConfig{
			Client: pubClient,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(middleware.Recoverer)
	router.AddMiddleware(common.NotifyMiddleware(publisher, serviceNameWithReplica))

	subClient := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	subscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client:        subClient,
			ConsumerGroup: "",
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	if replica == "1" {
		router.AddNoPublisherHandler(
			"OnUserSignedUp-2",
			"UserSignedUp-2",
			subscriber,
			func(msg *message.Message) error {
				var event common.UserSignedUp
				err := json.Unmarshal(msg.Payload, &event)
				if err != nil {
					return err
				}

				fmt.Println("Adding user", event.UserID, "to the CRM")

				return nil
			},
		)
	}

	if replica == "1" {
		_, err = cqrs.NewFacade(cqrs.FacadeConfig{
			GenerateEventsTopic: func(eventName string) string {
				return fmt.Sprintf("%s-8", eventName)
			},
			EventsPublisher: publisher,
			EventHandlers: func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.EventHandler {
				return []cqrs.EventHandler{
					cqrs.NewEventHandler("AddToCRM-8", AddToCRM8Handler{}.Handle),
					cqrs.NewEventHandler("AddToSupport-8", AddToSupport8Handler{}.Handle),
				}
			},
			EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
				handlerName = strings.Split(handlerName, "-")[0]
				return redisstream.NewSubscriber(
					redisstream.SubscriberConfig{
						Client:        subClient,
						ConsumerGroup: fmt.Sprintf("%s_%s", serviceName, handlerName),
					},
					logger,
				)
			},
			Router: router,
			CommandEventMarshaler: cqrs.JSONMarshaler{
				GenerateName: cqrs.StructName,
			},
			Logger: logger,
		})
		if err != nil {
			panic(err)
		}
	}

	_, err = cqrs.NewFacade(cqrs.FacadeConfig{
		GenerateEventsTopic: func(eventName string) string {
			return fmt.Sprintf("%s-9", eventName)
		},
		EventsPublisher: publisher,
		EventHandlers: func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.EventHandler {
			return []cqrs.EventHandler{
				cqrs.NewEventHandler("AddToCRM-9", AddToCRM9Handler{}.Handle),
				cqrs.NewEventHandler("AddToSupport-9", AddToSupport9Handler{}.Handle),
			}
		},
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			handlerName = strings.Split(handlerName, "-")[0]
			return redisstream.NewSubscriber(
				redisstream.SubscriberConfig{
					Client:        subClient,
					ConsumerGroup: fmt.Sprintf("%s_%s", serviceName, handlerName),
				},
				logger,
			)
		},
		Router: router,
		CommandEventMarshaler: cqrs.JSONMarshaler{
			GenerateName: cqrs.StructName,
		},
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}

type AddToCRM8Handler struct{}

func (h AddToCRM8Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	fmt.Println("Adding user", e.UserID, "to the CRM")

	return nil
}

type AddToSupport8Handler struct{}

func (h AddToSupport8Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	fmt.Println("Adding user", e.UserID, "to the support channel")

	return nil
}

type AddToCRM9Handler struct{}

func (h AddToCRM9Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	fmt.Println("Adding user", e.UserID, "to the CRM")

	return nil
}

type AddToSupport9Handler struct{}

func (h AddToSupport9Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	fmt.Println("Adding user", e.UserID, "to the support channel")

	return nil
}
