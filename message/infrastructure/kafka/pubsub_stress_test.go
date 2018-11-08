package kafka_test

import (
	"testing"

	"github.com/roblaszczak/gooddd/message/infrastructure"
)

func TestPublishSubscribe_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
		},
		createPubSub,
	)
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
		},
		createPartitionedPubSub,
	)
}
