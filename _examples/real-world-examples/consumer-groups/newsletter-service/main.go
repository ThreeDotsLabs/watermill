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
	serviceName            = "newsletter-service"
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

	newsletterServiceGroupSubscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client:        subClient,
			ConsumerGroup: "newsletter-service",
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	addToPromotionsListGroupSubscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client:        subClient,
			ConsumerGroup: "AddToPromotionsList",
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	addToNewsListGroupSubscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client:        subClient,
			ConsumerGroup: "AddToNewsList",
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	if replica == "1" {
		router.AddNoPublisherHandler(
			"OnUserSignedUp-1",
			"UserSignedUp-1",
			subscriber,
			func(msg *message.Message) error {
				var event common.UserSignedUp
				err := json.Unmarshal(msg.Payload, &event)
				if err != nil {
					return err
				}

				if !event.Consents.Marketing {
					return nil
				}

				fmt.Println("Adding user", event.UserID, "to the promotions list")

				return nil
			},
		)

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

				if !event.Consents.Marketing {
					return nil
				}

				fmt.Println("Adding user", event.UserID, "to the promotions list")

				return nil
			},
		)

		router.AddNoPublisherHandler(
			"AddToPromotionsList-5",
			"UserSignedUp-5",
			newsletterServiceGroupSubscriber,
			func(msg *message.Message) error {
				var event common.UserSignedUp
				err := json.Unmarshal(msg.Payload, &event)
				if err != nil {
					return err
				}

				if !event.Consents.Marketing {
					return nil
				}

				fmt.Println("Adding user", event.UserID, "to the promotions list")

				return nil
			},
		)

		router.AddNoPublisherHandler(
			"AddToNewsList-5",
			"UserSignedUp-5",
			newsletterServiceGroupSubscriber,
			func(msg *message.Message) error {
				var event common.UserSignedUp
				err := json.Unmarshal(msg.Payload, &event)
				if err != nil {
					return err
				}

				if !event.Consents.News {
					return nil
				}

				fmt.Println("Adding user", event.UserID, "to the news list")

				return nil
			},
		)

		router.AddNoPublisherHandler(
			"AddToPromotionsList-6",
			"UserSignedUp-6",
			addToPromotionsListGroupSubscriber,
			func(msg *message.Message) error {
				var event common.UserSignedUp
				err := json.Unmarshal(msg.Payload, &event)
				if err != nil {
					return err
				}

				if !event.Consents.Marketing {
					return nil
				}

				fmt.Println("Adding user", event.UserID, "to the promotions list")

				return nil
			},
		)

		router.AddNoPublisherHandler(
			"AddToNewsList-6",
			"UserSignedUp-6",
			addToNewsListGroupSubscriber,
			func(msg *message.Message) error {
				var event common.UserSignedUp
				err := json.Unmarshal(msg.Payload, &event)
				if err != nil {
					return err
				}

				if !event.Consents.News {
					return nil
				}

				fmt.Println("Adding user", event.UserID, "to the news list")

				return nil
			},
		)
	}

	router.AddNoPublisherHandler(
		"OnUserSignedUp-3",
		"UserSignedUp-3",
		subscriber,
		func(msg *message.Message) error {
			var event common.UserSignedUp
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			if !event.Consents.Marketing {
				return nil
			}

			fmt.Println("Adding user", event.UserID, "to the promotions list")

			return nil
		},
	)

	router.AddNoPublisherHandler(
		"OnUserSignedUp-4",
		"UserSignedUp-4",
		newsletterServiceGroupSubscriber,
		func(msg *message.Message) error {
			var event common.UserSignedUp
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			if !event.Consents.Marketing {
				return nil
			}

			fmt.Println("Adding user", event.UserID, "to the promotions list")

			return nil
		},
	)

	router.AddNoPublisherHandler(
		"AddToPromotionsList-7",
		"UserSignedUp-7",
		addToPromotionsListGroupSubscriber,
		func(msg *message.Message) error {
			var event common.UserSignedUp
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			if !event.Consents.Marketing {
				return nil
			}

			fmt.Println("Adding user", event.UserID, "to the promotions list")

			return nil
		},
	)

	router.AddNoPublisherHandler(
		"AddToNewsList-7",
		"UserSignedUp-7",
		addToNewsListGroupSubscriber,
		func(msg *message.Message) error {
			var event common.UserSignedUp
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			if !event.Consents.News {
				return nil
			}

			fmt.Println("Adding user", event.UserID, "to the news list")

			return nil
		},
	)

	if replica == "1" {
		_, err = cqrs.NewFacade(cqrs.FacadeConfig{
			GenerateEventsTopic: func(eventName string) string {
				return fmt.Sprintf("%s-8", eventName)
			},
			EventsPublisher: publisher,
			EventHandlers: func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.EventHandler {
				return []cqrs.EventHandler{
					cqrs.NewEventHandler("AddToPromotionsList-8", AddToPromotionsList8Handler{}.Handle),
					cqrs.NewEventHandler("AddToNewsList-8", AddToNewsList8Handler{}.Handle),
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
				cqrs.NewEventHandler("AddToPromotionsList-9", AddToPromotionsList9Handler{}.Handle),
				cqrs.NewEventHandler("AddToNewsList-9", AddToNewsList9Handler{}.Handle),
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

type AddToPromotionsList8Handler struct{}

func (h AddToPromotionsList8Handler) HandlerName() string {
	return "AddToPromotionsList-8"
}

func (h AddToPromotionsList8Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	if !e.Consents.Marketing {
		return nil
	}

	fmt.Println("Adding user", e.UserID, "to the promotions list")

	return nil
}

type AddToNewsList8Handler struct{}

func (h AddToNewsList8Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	if !e.Consents.News {
		return nil
	}
	fmt.Println("Adding user", e.UserID, "to the news list")

	return nil
}

type AddToPromotionsList9Handler struct{}

func (h AddToPromotionsList9Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	if !e.Consents.Marketing {
		return nil
	}

	fmt.Println("Adding user", e.UserID, "to the promotions list")

	return nil
}

type AddToNewsList9Handler struct{}

func (h AddToNewsList9Handler) Handle(ctx context.Context, e *common.UserSignedUp) error {
	if !e.Consents.News {
		return nil
	}

	fmt.Println("Adding user", e.UserID, "to the news list")

	return nil
}
