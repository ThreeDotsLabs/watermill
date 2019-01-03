package http

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/stretchr/testify/require"
)

func createPubSub(t *testing.T) message.PubSub {
	logger := watermill.NewStdLogger(true, true)

	// todo: the marshal/unmarshal functions
	// todo: a test http server that will service whatever requests are made in the tests

	pub, err := NewPublisher(nil, logger)
	require.NoError(t, err)

	sub, err := NewSubscriber("addr", nil, logger)

	return message.NewPubSub(pub, sub)
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
