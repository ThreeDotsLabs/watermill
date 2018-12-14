package gochannel_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func createPubSub(t *testing.T) message.PubSub {
	return gochannel.NewGoChannel(
		0,
		watermill.NewStdLogger(true, true),
		time.Second*10,
	)
}

func TestPublishSubscribe(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: true,
			GuaranteedOrder:     true,
			Persistent:          false,
		},
		createPubSub,
		nil,
	)
}
