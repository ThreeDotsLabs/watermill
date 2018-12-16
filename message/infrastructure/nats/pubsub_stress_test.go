// +build stress

package nats_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func TestPublishSubscribe_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithDurable,
	)
}
