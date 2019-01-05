package http

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func createPubSub(t *testing.T) message.PubSub {
	logger := watermill.NewStdLogger(true, true)

	sub, err := NewSubscriber(":8080", DefaultUnmarshalMessageFunc, logger)

	go func() {
		err := sub.StartHTTPServer()
		require.NoError(t, err)
	}()

	time.Sleep(200 * time.Millisecond)

	pub, err := NewPublisher(DefaultMarshalMessageFunc("http://localhost:8080"), logger)
	require.NoError(t, err)

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
