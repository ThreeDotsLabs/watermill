package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

type PostViewed struct {
	PostID int `json:"post_id"`
}

type PostReactionAdded struct {
	PostID     int    `json:"post_id"`
	ReactionID string `json:"reaction_id"`
}

type PostStatsUpdated struct {
	PostID          int     `json:"post_id"`
	ViewsUpdated    bool    `json:"views_updated"`
	ReactionUpdated *string `json:"reaction_updated"`
}

type Routers struct {
	EventsRouter *message.Router
	SSERouter    http.SSERouter
	EventBus     *cqrs.EventBus
}

func NewRouters(cfg config, repo *Repository) (Routers, error) {
	logger := watermill.NewStdLogger(false, false)

	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID: cfg.PubSubProjectID,
		},
		logger,
	)
	if err != nil {
		return Routers{}, err
	}

	eventBus, err := cqrs.NewEventBusWithConfig(
		publisher,
		cqrs.EventBusConfig{
			GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
				return params.EventName, nil
			},
			Marshaler: cqrs.JSONMarshaler{},
			Logger:    logger,
		},
	)
	if err != nil {
		return Routers{}, err
	}

	eventsRouter, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return Routers{}, err
	}

	eventsRouter.AddMiddleware(middleware.Recoverer)

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(
		eventsRouter,
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return params.EventName, nil
			},
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return googlecloud.NewSubscriber(
					googlecloud.SubscriberConfig{
						ProjectID: cfg.PubSubProjectID,
						GenerateSubscriptionName: func(topic string) string {
							return fmt.Sprintf("%v_%v", topic, params.HandlerName)
						},
					},
					logger,
				)
			},
			Marshaler: cqrs.JSONMarshaler{},
			Logger:    logger,
		},
	)
	if err != nil {
		return Routers{}, err
	}

	err = eventProcessor.AddHandlers(
		cqrs.NewEventHandler(
			"UpdateViews",
			func(ctx context.Context, event *PostViewed) error {
				err = repo.UpdatePost(ctx, event.PostID, func(post *Post) {
					post.Views++
				})
				if err != nil {
					return err
				}

				statsUpdated := PostStatsUpdated{
					PostID:       event.PostID,
					ViewsUpdated: true,
				}

				return eventBus.Publish(ctx, statsUpdated)
			},
		),
		cqrs.NewEventHandler(
			"UpdateReactions",
			func(ctx context.Context, event *PostReactionAdded) error {
				err := repo.UpdatePost(ctx, event.PostID, func(post *Post) {
					post.Reactions[event.ReactionID]++
				})
				if err != nil {
					return err
				}

				statsUpdated := PostStatsUpdated{
					PostID:          event.PostID,
					ReactionUpdated: &event.ReactionID,
				}

				return eventBus.Publish(ctx, statsUpdated)
			},
		),
	)
	if err != nil {
		return Routers{}, err
	}

	sseSubscriber, err := googlecloud.NewSubscriber(
		googlecloud.SubscriberConfig{
			ProjectID: cfg.PubSubProjectID,
			GenerateSubscriptionName: func(topic string) string {
				return fmt.Sprintf("%v_%v", topic, watermill.NewShortUUID())
			},
			SubscriptionConfig: pubsub.SubscriptionConfig{
				ExpirationPolicy: time.Hour * 24,
			},
		},
		logger,
	)
	if err != nil {
		return Routers{}, err
	}

	sseRouter, err := http.NewSSERouter(
		http.SSERouterConfig{
			UpstreamSubscriber: sseSubscriber,
			Marshaler:          http.StringSSEMarshaler{},
		},
		logger,
	)
	if err != nil {
		return Routers{}, err
	}

	return Routers{
		EventsRouter: eventsRouter,
		SSERouter:    sseRouter,
		EventBus:     eventBus,
	}, nil
}
