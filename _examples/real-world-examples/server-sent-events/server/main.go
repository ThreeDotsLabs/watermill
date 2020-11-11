package main

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
)

func main() {
	rand.Seed(time.Now().Unix())

	logger := watermill.NewStdLogger(false, false)

	postsStorage := NewPostsStorage()
	feedsStorage := NewFeedsStorage()

	pub, sub, err := SetupMessageRouter(feedsStorage, logger)
	if err != nil {
		panic(err)
	}

	httpRouter := Router{
		Subscriber:   sub,
		Publisher:    Publisher{publisher: pub},
		PostsStorage: postsStorage,
		FeedsStorage: feedsStorage,
		Logger:       logger,
	}

	mux := httpRouter.Mux()

	err = http.ListenAndServe(":8080", mux)
	if err != nil {
		panic(err)
	}
}
