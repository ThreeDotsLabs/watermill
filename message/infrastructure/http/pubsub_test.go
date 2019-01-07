package http

import (
	"net/http"
	"testing"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func createPubSub(t *testing.T) message.PubSub {
	logger := watermill.NewStdLogger(true, true)

	sub, err := NewSubscriber(":8080", SubscriberConfig{}, logger)

	errChan, err := sub.StartHTTPServer()
	require.NoError(t, err)
	go func() {
		err := <-errChan
		require.Equal(t, http.ErrServerClosed, errors.Cause(err))
	}()

	publisherConf := PublisherConfig{
		MarshalMessageFunc: DefaultMarshalMessageFunc("http://localhost:8080"),
	}

	pub, err := NewPublisher(publisherConf, logger)
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
