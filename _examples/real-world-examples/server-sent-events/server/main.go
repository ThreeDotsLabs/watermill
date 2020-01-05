package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/brianvoe/gofakeit"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	postsStorage := NewPostsStorage()
	feedsStorage := NewFeedsStorage()

	router, pub, sub, err := SetupMessageRouter(feedsStorage, logger)
	if err != nil {
		panic(err)
	}

	httpRouter := Router{
		MessageRouter: router,
		Subscriber:    sub,
		Publisher:     Publisher{publisher: pub},
		PostsStorage:  postsStorage,
		FeedsStorage:  feedsStorage,
		Logger:        logger,
	}

	mux := httpRouter.Mux()

	err = http.ListenAndServe(":8080", mux)
	if err != nil {
		panic(err)
	}
}

func runCommands(
	feedsStorage FeedsStorage,
) {
	for {
		uuid := watermill.NewUUID()
		title := gofakeit.Sentence(5)
		content := gofakeit.Sentence(20)
		author := gofakeit.Name()

		post := Post{
			ID:      uuid,
			Title:   title,
			Content: content,
			Author:  author,
			Tags:    []string{"cats", "golang", "misc"},
		}

		err := feedsStorage.AppendPost(context.Background(), post)
		if err != nil {
			log.Println("Error creating post:", err)
		}

		time.Sleep(time.Second * 1)

		post.Title += " (Updated)"
		post.Tags = []string{"cats", "golang"}

		err = feedsStorage.UpdatePost(context.Background(), post)
		if err != nil {
			log.Println("Error updating post:", err)
		}

		time.Sleep(time.Second * 3)
	}
}
