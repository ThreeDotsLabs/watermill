// +build stress

package gochannel_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPublishSubscribe_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:        false,
			ExactlyOnceDelivery:   true,
			GuaranteedOrder:       false,
			Persistent:            false,
			RequireSingleInstance: true,
		},
		createPersistentPubSub,
		nil,
	)
}
