package http_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/subscriber"

	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
)

func createPubSub(t *testing.T) (*http.Publisher, *http.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	// use any free port to allow parallel tests
	sub, err := http.NewSubscriber(":0", http.SubscriberConfig{}, logger)
	require.NoError(t, err)

	publisherConf := http.PublisherConfig{
		MarshalMessageFunc: http.DefaultMarshalMessageFunc,
	}

	pub, err := http.NewPublisher(publisherConf, logger)
	require.NoError(t, err)

	return pub, sub
}

func TestPublishSubscribe(t *testing.T) {
	t.Skip("todo - fix")

	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: true,
			GuaranteedOrder:     true,
			Persistent:          false,
		},
		nil,
		nil,
	)
}

func TestHttpPubSub(t *testing.T) {
	pub, sub := createPubSub(t)

	defer func() {
		require.NoError(t, pub.Close())
		require.NoError(t, sub.Close())
	}()

	msgs, err := sub.Subscribe("/test")
	require.NoError(t, err)

	go sub.StartHTTPServer()

	waitForHTTP(t, sub, time.Second*10)

	receivedMessages := make(chan message.Messages)

	go func() {
		received, _ := subscriber.BulkRead(msgs, 100, time.Second*10)
		receivedMessages <- received
	}()

	publishedMessages := infrastructure.AddSimpleMessages(t, 100, pub, fmt.Sprintf("http://%s/test", sub.Addr()))

	tests.AssertAllMessagesReceived(t, publishedMessages, <-receivedMessages)
}

func waitForHTTP(t *testing.T, sub *http.Subscriber, timeoutTime time.Duration) {
	timeout := time.After(timeoutTime)
	for {
		addr := sub.Addr()
		if addr != nil {
			break
		}

		select {
		case <-timeout:
			t.Fatal("server not up")
		default:
			// ok
		}

		time.Sleep(time.Millisecond * 10)
	}
}
