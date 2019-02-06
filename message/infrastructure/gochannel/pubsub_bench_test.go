package gochannel_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func BenchmarkSubscriber(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) message.PubSub {
		return gochannel.NewGoChannel(int64(n), watermill.NopLogger{})
	})
}

func BenchmarkSubscriberPersistent(b *testing.B) {
	infrastructure.BenchSubscriber(b, func(n int) message.PubSub {
		return gochannel.NewPersistentGoChannel(int64(n), watermill.NopLogger{})
	})
}
