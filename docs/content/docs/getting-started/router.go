package main

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	logger = watermill.NewStdLogger(false, false)
)

func main() {
	// for simplicity we are using gochannel Pub/Sub here,
	// but every implementation of PubSub interface will work.
	pubSub := gochannel.NewGoChannel(
		0, // buffer (channel) size
		watermill.NewStdLogger(false, false),
		time.Second, // send timeout
	)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	// this plugin will gracefully shutdown router, when SIGTERM was sent
	// you can also close router by just calling `r.Close()`
	router.AddPlugin(plugin.SignalsHandler)

	router.AddMiddleware(
		// when error occurred, function will be retried,
		// after max retries (or if no Retry middleware is added) Nack is send and message will be resent
		middleware.Retry{
			MaxRetries: 3,
			WaitTime:   time.Second,
			Backoff:    3,
			Logger:     logger,
		}.Middleware,

		// this middleware will handle panics from handlers
		// and pass them as error to retry middleware in this case
		middleware.Recoverer,
	)

	router.AddHandler("print", "shop.events", "shop.events", pubSub, func(msg *message.Message) (messages []*message.Message, e error) {

	})
}
