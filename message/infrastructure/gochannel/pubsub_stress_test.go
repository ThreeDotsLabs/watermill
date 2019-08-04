// +build stress

package gochannel_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func TestPublishSubscribe_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
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
