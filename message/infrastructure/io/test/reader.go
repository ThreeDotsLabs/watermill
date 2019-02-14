package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/io"
)

var random = rand.New(rand.NewSource(time.Now().Unix()))

func main() {
	sub, err := io.NewSubscriber(os.Stdin, io.SubscriberConfig{
		UnmarshalFunc: io.PayloadUnmarshalFunc,
	})
	if err != nil {
		panic(err)
	}

	messages, err := sub.Subscribe(context.Background(), "stdin")
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		fmt.Printf("[%s] %s: %s", time.Now().Format(time.RFC3339), msg.UUID, msg.Payload)
		// todo: this ain't workin
		if rand.Float32() < 0.1 {
			msg.Nack()
			continue
		}
		msg.Ack()
	}
}
