// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	// just a simplest implementation,
	// probably you want to ship your own implementation of `watermill.LoggerAdapter`
	logger = watermill.NewStdLogger(false, false)
)

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	// this plugin will gracefully shutdown router, when SIGTERM was sent
	// you can also close router by just calling `r.Close()`
	router.AddPlugin(plugin.SignalsHandler)

	router.AddMiddleware(
		// correlation ID will copy correlation id from consumed message metadata to produced messages
		middleware.CorrelationID,

		// when error occurred, function will be retried,
		// after max retries (or if no Retry middleware is added) Nack is send and message will be resent
		middleware.Retry{
			MaxRetries: 3,
			WaitTime:   time.Millisecond * 100,
			Backoff:    3,
			Logger:     logger,
		}.Middleware,

		// this middleware will handle panics from handlers
		// and pass them as error to retry middleware in this case
		middleware.Recoverer,
	)

	// for simplicity we are using gochannel Pub/Sub here,
	// you can replace it with any Pub/Sub implementation, it will work the same
	pubSub := gochannel.NewGoChannel(0, logger, time.Second)

	// producing some messages in background
	go publishMessages(pubSub)

	if err := router.AddHandler(
		"struct_handler",  // handler name, must be unique
		"example.topic_1", // topic from which we will read events
		"example.topic_2", // topic to which we will publish event
		pubSub,
		structHandler{}.Handler,
	); err != nil {
		panic(err)
	}

	// just for debug, we are printing all events sent to `example.topic_1`
	if err := router.AddNoPublisherHandler(
		"print_events_topic_1",
		"example.topic_1",
		pubSub,
		printMessages,
	); err != nil {
		panic(err)
	}

	// just for debug, we are printing all events sent to `example.topic_2`
	if err := router.AddNoPublisherHandler(
		"print_events_topic_2",
		"example.topic_2",
		pubSub,
		printMessages,
	); err != nil {
		panic(err)
	}

	// when everything is ready, let's run router,
	// this function is blocking since router is running
	if err := router.Run(); err != nil {
		panic(err)
	}
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(uuid.NewV4().String(), []byte("Hello, world!"))
		middleware.SetCorrelationID(uuid.NewV4().String(), msg)

		log.Printf("sending message %s, correlation id: %s\n", msg.UUID, middleware.MessageCorrelationID(msg))

		if err := publisher.Publish("example.topic_1", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func printMessages(msg *message.Message) ([]*message.Message, error) {
	fmt.Printf(
		"\n> Received message: %s\n> %s\n> metadata: %v\n\n",
		msg.UUID, string(msg.Payload), msg.Metadata,
	)
	return nil, nil
}

type structHandler struct {
	// we can add some dependencies here
}

func (s structHandler) Handler(msg *message.Message) ([]*message.Message, error) {
	log.Println("structHandler received message", msg.UUID)

	msg = message.NewMessage(uuid.NewV4().String(), []byte("message produced by structHandler"))
	return message.Messages{msg}, nil
}
