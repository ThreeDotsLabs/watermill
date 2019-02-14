package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/io"
)

func main() {
	pub, err := io.NewPublisher(os.Stdout, io.PublisherConfig{
		MarshalFunc: io.PrettyPayloadMarshalFunc,
	})

	if err != nil {
		panic(err)
	}

	sub, err := http.NewSubscriber(
		":8080",
		http.SubscriberConfig{},
		watermill.NewStdLoggerWithOut(os.Stderr, true, true),
	)

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	messages, err := sub.Subscribe(ctx, "/posts")
	if err != nil {
		panic(err)
	}

	go func() {
		err := sub.StartHTTPServer()
		if err != nil {
			panic(err)
		}
	}()

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
		if err := sub.Close(); err != nil {
			panic(err)
		}

		if err := pub.Close(); err != nil {
			panic(err)
		}
	}()

	for msg := range messages {
		err := pub.Publish("sometopic", msg)
		if err != nil {
			panic(err)
		}
		msg.Ack()
	}
}
