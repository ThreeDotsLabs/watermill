package gochannel_test

import (
	"context"
	"fmt"
	"log"
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

func createPersistentPubSubWithContextPreserved(t *testing.T) (message.Publisher, message.Subscriber) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			OutputChannelBuffer: 10000,
			Persistent:          true,
			PreserveContext:     true,
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

func TestPublishSubscribe_context_preserved(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:        false,
			ExactlyOnceDelivery:   true,
			GuaranteedOrder:       false,
			Persistent:            false,
			RequireSingleInstance: true,
			ContextPreserved:      true,
		},
		createPersistentPubSubWithContextPreserved,
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

func TestPublishSubscribe_not_persistent_with_context(t *testing.T) {
	messagesCount := 100
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{OutputChannelBuffer: int64(messagesCount), PreserveContext: true},
		watermill.NewStdLogger(true, true),
	)
	topicName := "test_topic_" + watermill.NewUUID()

	msgs, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	const contextKeyString = "foo"
	sendMessages := tests.PublishSimpleMessagesWithContext(t, messagesCount, contextKeyString, pubSub, topicName)
	receivedMsgs, _ := subscriber.BulkRead(msgs, messagesCount, time.Second)

	expectedContexts := make(map[string]context.Context)
	for _, msg := range sendMessages {
		expectedContexts[msg.UUID] = msg.Context()
	}
	tests.AssertAllMessagesReceived(t, sendMessages, receivedMsgs)
	tests.AssertAllMessagesHaveSameContext(t, contextKeyString, expectedContexts, receivedMsgs)

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
				_ = pubSub.Publish("topic", message.NewMessage(watermill.NewShortUUID(), nil))
			}()

			err := pubSub.Close()
			require.NoError(t, err)
		})
	}
}

func TestPublishSubscribe_do_not_block_other_subscribers(t *testing.T) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		watermill.NewStdLogger(true, true),
	)
	topicName := "test_topic_" + watermill.NewUUID()

	msgsFromSubscriber1, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	_, err = pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	msgsFromSubscriber3, err := pubSub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	err = pubSub.Publish(topicName, message.NewMessage("1", nil))
	require.NoError(t, err)

	received := make(chan struct{})
	go func() {
		msg := <-msgsFromSubscriber1
		msg.Ack()

		msg = <-msgsFromSubscriber3
		msg.Ack()

		close(received)
	}()

	select {
	case <-received:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber which didn't ack a message blocked other subscribers from receiving it")
	}
}

func TestPublishSubscribe_flush_output_channel(t *testing.T) {
	messagesCount := 300
	logger := watermill.NewStdLogger(true, true)
	ctx := context.Background()
	config := gochannel.Config{
		OutputChannelBuffer:            int64(messagesCount),
		Persistent:                     false,
		BlockPublishUntilSubscriberAck: false,
	}
	pubSub := gochannel.NewGoChannel(
		config,
		logger,
	)

	totalMessage := 0
	artificialWorkload := time.Millisecond * 5 //keep it small but noticeable in logs
	topicName := "test_topic"

	messageChannel, err := pubSub.Subscribe(ctx, topicName)
	if err != nil {
		t.Fatal(err)
	}

	// waitgroup for stopping the subscriber handler from processing/receiving messages until we are done filling the buffer of the pubSub
	var wgStartSubscriber sync.WaitGroup
	wgStartSubscriber.Add(1)
	// waitgroup for expected buffer to be flushed
	var wgFlushBuffer sync.WaitGroup
	wgFlushBuffer.Add(messagesCount)

	// start subscriber handler in a go routine
	// reads out the messages, if all is ok it should be able to ("flush") read all messages from a 'closed' pubsub
	go func(messageChannel <-chan *message.Message) {
		wgStartSubscriber.Wait()
		for msg := range messageChannel {
			// artificial workload
			time.Sleep(artificialWorkload)
			msg.Ack()
			logger.Trace("message acked", nil)
			// would normally use atomic value here but concurrency shouldn't be an issue for this test
			totalMessage++
			wgFlushBuffer.Done()
		}
		logger.Trace("channel closed", nil)
	}(messageChannel)

	tests.PublishSimpleMessages(t, messagesCount, pubSub, topicName)
	// wait for buffer to fill then start reading in subscriber handler
	for {
		if len(messageChannel) != int(config.OutputChannelBuffer) {
			continue
		} else {
			wgStartSubscriber.Done()
			break
		}
	}

	err = pubSub.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Publishing new message should still error as expected
	err = pubSub.Publish(topicName, message.NewMessage(watermill.NewUUID(), []byte("x")))
	assert.ErrorContains(t, err, "Pub/Sub closed")

	// And so should subscribe
	_, err = pubSub.Subscribe(ctx, topicName)
	assert.ErrorContains(t, err, "Pub/Sub closed")

	wgFlushBuffer.Wait()
	// But subscriber handler should still be able to read the remaining messages from output channel aka flushing
	assert.Equal(t, messagesCount, totalMessage)
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
