package gochannel_test

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func createPersistentPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			OutputChannelBuffer: 10000,
			Persistent:          true,
		},
		watermill.NewStdLogger(true, true),
	)
	return pubSub, pubSub
}

func TestPublishSubscribe_persistent(t *testing.T) {
	tests.TestPubSub(
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

func TestPublishSubscribe_not_persistent(t *testing.T) {
	messagesCount := 100
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{OutputChannelBuffer: int64(messagesCount)},
		watermill.NewStdLogger(true, true),
	)
	topicName := "test_topic_" + watermill.NewUUID()

	msgs, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	sendMessages := tests.PublishSimpleMessages(t, messagesCount, pubSub, topicName)
	receivedMsgs, _ := subscriber.BulkRead(msgs, messagesCount, time.Second)

	tests.AssertAllMessagesReceived(t, sendMessages, receivedMsgs)

	assert.NoError(t, pubSub.Close())
}

func TestPublishSubscribe_block_until_ack(t *testing.T) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: true},
		watermill.NewStdLogger(true, true),
	)
	topicName := "test_topic_" + watermill.NewUUID()

	msgs, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	published := make(chan struct{})
	go func() {
		err := pubSub.Publish(topicName, message.NewMessage("1", nil))
		require.NoError(t, err)
		close(published)
	}()

	msg1 := <-msgs
	select {
	case <-published:
		t.Fatal("publish should be blocked until ack")
	default:
		// ok
	}

	msg1.Nack()
	select {
	case <-published:
		t.Fatal("publish should be blocked after nack")
	default:
		// ok
	}

	msg2 := <-msgs
	msg2.Ack()

	select {
	case <-published:
		// ok
	case <-time.After(time.Second):
		t.Fatal("publish should be not blocked after ack")
	}
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

func TestSubscribe_race_condition_when_closing(t *testing.T) {
	testsCount := 15
	if testing.Short() {
		testsCount = 3
	}

	for i := 0; i < testsCount; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			pubSub := gochannel.NewGoChannel(
				gochannel.Config{},
				watermill.NewStdLogger(true, false),
			)
			go func() {
				err := pubSub.Close()
				require.NoError(t, err)
			}()
			_, err := pubSub.Subscribe(context.Background(), "topic")
			require.NoError(t, err)
		})
	}
}

func TestPublish_race_condition_when_closing(t *testing.T) {
	testsCount := 15
	if testing.Short() {
		testsCount = 3
	}

	for i := 0; i < testsCount; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel()
			pubSub := gochannel.NewGoChannel(
				gochannel.Config{},
				watermill.NewStdLogger(true, false),
			)
			go func() {
				err := pubSub.Close()
				require.NoError(t, err)
			}()
			err := pubSub.Publish("topic", message.NewMessage(strconv.Itoa(i), nil))
			require.NoError(t, err)
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

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			OutputChannelBuffer: int64(messagesCount),
			Persistent:          true,
		},
		watermill.NewStdLogger(true, false),
	)

	allSent := sync.WaitGroup{}
	allSent.Add(messagesCount)
	allReceived := sync.WaitGroup{}

	sentMessages := message.Messages{}
	go func() {
		for i := 0; i < messagesCount; i++ {
			msg := message.NewMessage(watermill.NewUUID(), nil)
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
			msgs, err := pubSub.Subscribe(context.Background(), "topic")
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
