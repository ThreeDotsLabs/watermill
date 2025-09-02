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
		router.AddConsumerHandler(
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

		router.AddConsumerHandler(
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

		router.AddConsumerHandler(
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

		router.AddConsumerHandler(
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

		router.AddConsumerHandler(
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

		router.AddConsumerHandler(
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

	router.AddConsumerHandler(
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

	router.AddConsumerHandler(
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

	router.AddConsumerHandler(
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

	router.AddConsumerHandler(
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
		eventProc8, err := cqrs.NewEventProcessorWithConfig(router, cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return fmt.Sprintf("%s-8", params.EventName), nil
			},
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				handlerName := strings.Split(params.HandlerName, "-")[0]
				return redisstream.NewSubscriber(
					redisstream.SubscriberConfig{
						Client:        subClient,
						ConsumerGroup: fmt.Sprintf("%s_%s", serviceName, handlerName),
					},
					logger,
				)
			},
			Marshaler: cqrs.JSONMarshaler{
				GenerateName: cqrs.StructName,
			},
			Logger: logger,
		})
		if err != nil {
			panic(err)
		}
		err = eventProc8.AddHandlers(
			cqrs.NewEventHandler("AddToPromotionsList-8", HandlePromotions),
			cqrs.NewEventHandler("AddToNewsList-8", HandleNews),
		)
		if err != nil {
			panic(err)
		}
	}

	eventProc9, err := cqrs.NewEventProcessorWithConfig(router, cqrs.EventProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
			return fmt.Sprintf("%s-9", params.EventName), nil
		},
		SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			handlerName := strings.Split(params.HandlerName, "-")[0]
			return redisstream.NewSubscriber(
				redisstream.SubscriberConfig{
					Client:        subClient,
					ConsumerGroup: fmt.Sprintf("%s_%s", serviceName, handlerName),
				},
				logger,
			)
		},
		Marshaler: cqrs.JSONMarshaler{
			GenerateName: cqrs.StructName,
		},
		Logger: logger,
	})
	if err != nil {
		panic(err)
	}

	err = eventProc9.AddHandlers(
		cqrs.NewEventHandler("AddToPromotionsList-9", HandlePromotions),
		cqrs.NewEventHandler("AddToNewsList-9", HandleNews),
	)
	if err != nil {
		panic(err)
	}

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}

func HandleNews(ctx context.Context, e *common.UserSignedUp) error {
	if !e.Consents.News {
		return nil
	}

	fmt.Println("Adding user", e.UserID, "to the news list")

	return nil
}

func HandlePromotions(ctx context.Context, e *common.UserSignedUp) error {
	if !e.Consents.Marketing {
		return nil
	}

	fmt.Println("Adding user", e.UserID, "to the promotions list")

	return nil
}
