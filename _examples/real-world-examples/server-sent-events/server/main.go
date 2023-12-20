package main

import (
	"net/http"

	"github.com/ascendsoftware/watermill"
)

func main() {
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
