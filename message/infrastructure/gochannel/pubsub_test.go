package gochannel_test

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal/tests"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/stretchr/testify/require"
)

func createPersistentPubSub(t *testing.T) infrastructure.PubSub {
	return gochannel.NewPersistentGoChannel(
		10000,
		watermill.NewStdLogger(true, true),
	).(infrastructure.PubSub)
}

func TestPublishSubscribe_persistent(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: true,
			GuaranteedOrder:     false,
			Persistent:          false,
		},
		createPersistentPubSub,
		nil,
	)
}

func TestPublishSubscribe_not_persistent(t *testing.T) {
	messagesCount := 100
	pubSub := gochannel.NewGoChannel(
		int64(messagesCount),
		watermill.NewStdLogger(true, true),
	)
	topicName := "test_topic_" + watermill.UUID()

	msgs, err := pubSub.Subscribe(topicName)
	require.NoError(t, err)

	sendMessages := infrastructure.AddSimpleMessages(t, messagesCount, pubSub, topicName)
	receivedMsgs, _ := subscriber.BulkRead(msgs, messagesCount, time.Second)

	tests.AssertAllMessagesReceived(t, sendMessages, receivedMsgs)
}

func TestPublishSubscribe_race_condition_on_subscribe(t *testing.T) {
	testsCount := 15
	if testing.Short() {
		testsCount = 3
	}

	for i := 0; i < testsCount; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			testPublishSubscribeSubRace(t)
		})
	}
}

func testPublishSubscribeSubRace(t *testing.T) {
	t.Helper()

	messagesCount := 500
	subscribersCount := 200
	if testing.Short() {
		messagesCount = 200
		subscribersCount = 20
	}

	pubSub := gochannel.NewPersistentGoChannel(
		int64(messagesCount),
		watermill.NewStdLogger(true, false),
	)

	allSent := sync.WaitGroup{}
	allSent.Add(messagesCount)
	allReceived := sync.WaitGroup{}

	sentMessages := message.Messages{}
	go func() {
		for i := 0; i < messagesCount; i++ {
			msg := message.NewMessage(watermill.UUID(), nil)
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

	log.Println("waiting for all sent")
	allSent.Wait()

	log.Println("waiting for all received")
	allReceived.Wait()

	close(subscriberReceivedCh)

	log.Println("asserting")

	for subMsgs := range subscriberReceivedCh {
		tests.AssertAllMessagesReceived(t, sentMessages, subMsgs)
	}
}
