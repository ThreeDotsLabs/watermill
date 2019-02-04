// +build reconnect

package amqp_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func TestPublishSubscribe_reconnect(t *testing.T) {
	infrastructure.TestReconnect(t, createPubSub(t), infrastructure.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	})
}
