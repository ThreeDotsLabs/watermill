// +build stress

package amqp_test

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
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createQueuePubSub,
		nil,
	)
}
