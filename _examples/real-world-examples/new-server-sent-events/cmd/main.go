package main

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"

	"main.go/pkg/adapters/events"
	"main.go/pkg/adapters/feed"
	"main.go/pkg/adapters/post"
	"main.go/pkg/app/command"
	"main.go/pkg/ports/pubsub"
)

func main() {
	logger := watermill.NewStdLogger(true, false)

	goChannelPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(middleware.Recoverer)

	postRepository := &post.MemoryRepository{}
	feedRepository := &feed.MemoryRepository{}

	cqrsFacade, err := cqrs.NewFacade(cqrs.FacadeConfig{
		GenerateEventsTopic: func(eventName string) string {
			return eventName
		},
		EventHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
			publisher := events.Publisher{eb}

			addPostToFeedsHandler := command.NewAddPostToFeedsHandler(
				feedRepository,
				publisher,
			)

			updatePostInFeedsHandler := command.NewUpdatePostInFeedsHandler(
				feedRepository,
				publisher,
			)

			return []cqrs.EventHandler{
				pubsub.PostCreatedHandler{addPostToFeedsHandler},
				pubsub.PostUpdatedHandler{updatePostInFeedsHandler},
				pubsub.FeedUpdatedHandler{},
			}
		},
		EventsPublisher: goChannelPubSub,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			return gochannel.NewGoChannel(gochannel.Config{}, logger), nil
		},
		Router:                router,
		CommandEventMarshaler: cqrs.JSONMarshaler{},
		Logger:                logger,
	})
	if err != nil {
		panic(err)
	}

	publisher := events.Publisher{cqrsFacade.EventBus()}

	createPostHandler := command.NewCreatePostHandler(postRepository, publisher)
	updatePostHandler := command.NewUpdatePostHandler(postRepository, publisher)

	_ = createPostHandler
	_ = updatePostHandler

	if err != nil {
		panic(err)
	}

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}
