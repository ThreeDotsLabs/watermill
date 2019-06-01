package gochannel_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
)

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{OutputChannelBuffer: int64(n)},
		)
		return pubSub, pubSub
	})
}

func BenchmarkSubscriberPersistent(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		pubSub := gochannel.NewGoChannel(
			gochannel.Config{
				OutputChannelBuffer: int64(n),
				Persistent:          true,
			},
		)
		return pubSub, pubSub
	})
}
