package gochannel_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/internal/tests"

	"github.com/ThreeDotsLabs/watermill/message/subscriber"

	"github.com/stretchr/testify/require"

	"github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

// todo - test not persistent

func createPubSub(t *testing.T) message.PubSub {
	return gochannel.NewPersistentGoChannel(
		10000,
		watermill.NewStdLogger(true, true),
		time.Second*10,
	)
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

func TestPublishSubscribe_race_condition_on_subscribe(t *testing.T) {
	for i := 0; i < 15; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			testPublishSubscribeSubRace(t)
		})
	}
}

func testPublishSubscribeSubRace(t *testing.T) {
	t.Helper()

	const messagesCount = 1000
	const subscribersCount = 200

	pubSub := gochannel.NewPersistentGoChannel(
		messagesCount,
		watermill.NewStdLogger(true, false),
		time.Second*10,
	)

	allSent := sync.WaitGroup{}
	allSent.Add(messagesCount)
	allReceived := sync.WaitGroup{}

	sentMessages := message.Messages{}
	go func() {
		for i := 0; i < messagesCount; i++ {
			msg := message.NewMessage(uuid.NewV4().String(), nil)
			sentMessages = append(sentMessages, msg)

			go func() {
				require.NoError(t, pubSub.Publish("topic", msg))
				allSent.Done()
			}()
		}
	}()

	subscriberReceivedCh := make(chan message.Messages, subscribersCount)
	for i := 0; i < subscribersCount; i++ {
		allReceived.Add(1)

		go func() {
			msgs, err := pubSub.Subscribe("topic")
			require.NoError(t, err)

			received, _ := subscriber.BulkRead(msgs, messagesCount, time.Second*10)
			subscriberReceivedCh <- received

			allReceived.Done()
		}()
	}

	fmt.Println("waiting for all sent")
	allSent.Wait()

	fmt.Println("waiting for all received")
	allReceived.Wait()

	close(subscriberReceivedCh)

	fmt.Println("asserting")

	for subMsgs := range subscriberReceivedCh {
		tests.AssertAllMessagesReceived(t, sentMessages, subMsgs)
	}
}
