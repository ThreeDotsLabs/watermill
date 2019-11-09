package main

import (
	"context"
	"log"
	stdHTTP "net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/brianvoe/gofakeit"

	"main.go/pkg/adapters/events"
	"main.go/pkg/adapters/feed"
	"main.go/pkg/adapters/post"
	"main.go/pkg/app/command"
	"main.go/pkg/app/model"
	"main.go/pkg/app/query"
	"main.go/pkg/ports/http"
	"main.go/pkg/ports/pubsub"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	internalPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
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
			}
		},
		EventsPublisher: goChannelPubSub,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			return goChannelPubSub, nil
		},
		Router:                router,
		CommandEventMarshaler: cqrs.JSONMarshaler{},
		Logger:                logger,
	})
	if err != nil {
		panic(err)
	}

	topicFunc := func(event interface{}) string {
		return cqrs.JSONMarshaler{}.Name(event)
	}

	topic := topicFunc(model.FeedUpdated{})
	router.AddHandler(
		"feed-updated",
		topic,
		goChannelPubSub,
		topic,
		internalPubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			return []*message.Message{msg}, nil
		},
	)

	publisher := events.Publisher{cqrsFacade.EventBus()}

	createPostHandler := command.NewCreatePostHandler(postRepository, publisher)
	updatePostHandler := command.NewUpdatePostHandler(postRepository, publisher)

	// --
	err = feedRepository.Add(model.Feed{
		ID: 1,
	})
	if err != nil {
		panic(err)
	}
	go runCommands(createPostHandler, updatePostHandler)
	// --

	feedByIDHandler := query.FeedByIDHandler{
		FeedByIDFinder: feedRepository,
	}

	httpRouter := http.Router{
		Subscriber:      internalPubSub,
		TopicFunc:       topicFunc,
		FeedByIDHandler: feedByIDHandler,
	}

	go func() {
		if err := stdHTTP.ListenAndServe(":8080", httpRouter.Mux()); err != nil {
			panic(err)
		}
	}()

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

func runCommands(
	createPostHandler command.CreatePostHandler,
	updatePostHandler command.UpdatePostHandler,
) {
	for {
		uuid := watermill.NewUUID()
		title := gofakeit.Sentence(5)
		content := gofakeit.Sentence(20)
		author := gofakeit.Name()

		err := createPostHandler.Execute(command.CreatePost{
			UUID:    uuid,
			Title:   title,
			Content: content,
			Author:  author,
		})

		if err != nil {
			log.Println("Error creating post:", err)
		}

		time.Sleep(time.Second * 1)

		err = updatePostHandler.Execute(command.UpdatePost{
			UUID:    uuid,
			Title:   title + " (Updated)",
			Content: content,
			Author:  author,
		})

		if err != nil {
			log.Println("Error updating post:", err)
		}

		time.Sleep(time.Second * 3)
	}
}
