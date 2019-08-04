package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
)

type BenchmarkPubSubConstructor func(n int) (message.Publisher, message.Subscriber)

func BenchSubscriber(b *testing.B, pubSubConstructor BenchmarkPubSubConstructor) {
	pub, sub := pubSubConstructor(b.N)
	topicName := testTopicName(NewTestID())

	messages, err := sub.Subscribe(context.Background(), topicName)
	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for i := 0; i < b.N; i++ {
			msg := message.NewMessage("1", nil)
			err := pub.Publish(topicName, msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	b.ResetTimer()

	consumedMessages, all := subscriber.BulkRead(messages, b.N, time.Second*60)
	if !all {
		b.Fatalf("not all messages received, have %d, expected %d", len(consumedMessages), b.N)
	}
}
