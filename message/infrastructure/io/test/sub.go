package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/io"
)

func main() {
	file, err := os.Open("/tmp/test.log")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	sub, err := io.NewSubscriber(file, io.SubscriberConfig{
		//BufferSize:    32,
		MessageDelimiter: '\n',
		Logger:           watermill.NewStdLogger(true, true),
		UnmarshalFunc:    io.PayloadUnmarshalFunc,
	})
	if err != nil {
		panic(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Minute)

	msgChan, err := sub.Subscribe(ctx, "topic")
	for msg := range msgChan {
		fmt.Printf("%s:\t%s", msg.UUID, msg.Payload)
		msg.Ack()
	}

	err = sub.Close()
	if err != nil {
		panic(err)
	}
}
