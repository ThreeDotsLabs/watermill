package main

import (
	"context"
	"net/http"
	"sync"

	"github.com/ascendsoftware/watermill"
	"github.com/ascendsoftware/watermill-redisstream/pkg/redisstream"
	"github.com/ascendsoftware/watermill-routing-example/server/common"
	"github.com/ascendsoftware/watermill/message"
	"github.com/ascendsoftware/watermill/message/router/middleware"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(middleware.Recoverer)

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

	subClient := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	subscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client: subClient,
		},
		logger,
	)

	go func() {
		err = router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-router.Running()

	storage := &storage{
		lock:             &sync.Mutex{},
		receivedMessages: map[string][]common.MessageReceived{},
	}

	httpRouter := Handler{
		storage:    storage,
		subscriber: subscriber,
		publisher:  publisher,
		logger:     logger,
		lastIDs:    map[string]int{},
	}

	err = http.ListenAndServe(":8080", httpRouter.Mux())
	if err != nil {
		panic(err)
	}
}
