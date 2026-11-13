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

// A Publish waiting for the subscribers' ack must not block Subscribe on other topics.
// Otherwise, the Subscribe blocks the following Publish calls on all topics,
// including the ones made by the handler which didn't ack the message yet.
func TestPublishSubscribe_block_until_ack_subscribe_to_other_topic(t *testing.T) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: true},
		watermill.NewStdLogger(true, true),
	)
	defer func() { assert.NoError(t, pubSub.Close()) }()
	topicName := "test_topic_" + watermill.NewUUID()

	messages := subscribeWithTimeout(t, pubSub, topicName)

	published := publishAsync(pubSub, topicName, message.NewMessage("1", nil))
	msg := receiveWithTimeout(t, messages)

	_ = subscribeWithTimeout(t, pubSub, "other_topic_"+watermill.NewUUID())

	// The handler publishes to another topic before acking the message, like the Router.
	requirePublished(t, publishAsync(pubSub, "another_topic_"+watermill.NewUUID(), message.NewMessage("2", nil)))
	msg.Ack()

	requirePublished(t, published)
}

func TestPublishSubscribe_block_until_ack_one_message_per_topic_at_a_time(t *testing.T) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: true},
		watermill.NewStdLogger(true, true),
	)
	defer func() { assert.NoError(t, pubSub.Close()) }()
	topicName := "test_topic_" + watermill.NewUUID()

	messages := subscribeWithTimeout(t, pubSub, topicName)

	published1 := publishAsync(pubSub, topicName, message.NewMessage("1", nil))
	msg1 := receiveWithTimeout(t, messages)

	published2 := publishAsync(pubSub, topicName, message.NewMessage("2", nil))
	requireNoMessage(t, messages)

	msg1.Ack()
	requirePublished(t, published1)

	receiveWithTimeout(t, messages).Ack()
	requirePublished(t, published2)
}

// Subscribe waited for the Publish in progress on its topic before the deadlock fix,
// and the fix keeps that, so a new subscriber never joins in the middle of a Publish call.
func TestPublishSubscribe_block_until_ack_subscribe_to_same_topic(t *testing.T) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: true},
		watermill.NewStdLogger(true, true),
	)
	defer func() { assert.NoError(t, pubSub.Close()) }()
	topicName := "test_topic_" + watermill.NewUUID()

	messages := subscribeWithTimeout(t, pubSub, topicName)

	published := publishAsync(pubSub, topicName, message.NewMessage("1", nil), message.NewMessage("2", nil))
	msg1 := receiveWithTimeout(t, messages)

	subscribed := make(chan (<-chan *message.Message), 1)
	go func() {
		newSubscriberMessages, err := pubSub.Subscribe(context.Background(), topicName)
		assert.NoError(t, err)
		subscribed <- newSubscriberMessages
	}()

	select {
	case <-subscribed:
		t.Fatal("Subscribe should wait for the Publish on the same topic")
	case <-time.After(50 * time.Millisecond):
		// ok
	}

	msg1.Ack()
	receiveWithTimeout(t, messages).Ack()
	requirePublished(t, published)

	select {
	case newSubscriberMessages := <-subscribed:
		requireNoMessage(t, newSubscriberMessages)
	case <-time.After(deadlockTimeout):
		t.Fatalf("Subscribe didn't return within %s after Publish returned", deadlockTimeout)
	}
}

// Publish releases subscribersLock between its messages now, so a subscriber joining during it
// must still get each message exactly once, not both persisted and sent, and none missing.
func TestPublishSubscribe_block_until_ack_persistent_subscribe_during_publish(t *testing.T) {
	messagesCount := 20

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			Persistent:                     true,
			BlockPublishUntilSubscriberAck: true,
		},
		watermill.NewStdLogger(true, true),
	)
	defer func() { assert.NoError(t, pubSub.Close()) }()
	topicName := "test_topic_" + watermill.NewUUID()

	firstSubscriberMessages := subscribeWithTimeout(t, pubSub, topicName)

	messagesToPublish := make(message.Messages, messagesCount)
	for i := range messagesToPublish {
		messagesToPublish[i] = message.NewMessage(watermill.NewUUID(), nil)
	}
	published := publishAsync(pubSub, topicName, messagesToPublish...)

	receivedByFirstSubscriber := message.Messages{receiveWithTimeout(t, firstSubscriberMessages)}

	receivedByNewSubscriber := make(chan message.Messages, 1)
	go func() {
		newSubscriberMessages, err := pubSub.Subscribe(context.Background(), topicName)
		if !assert.NoError(t, err) {
			return
		}
		received, _ := subscriber.BulkRead(newSubscriberMessages, messagesCount, deadlockTimeout)
		receivedByNewSubscriber <- received
	}()

	receivedByFirstSubscriber[0].Ack()
	for i := 1; i < messagesCount; i++ {
		msg := receiveWithTimeout(t, firstSubscriberMessages)
		receivedByFirstSubscriber = append(receivedByFirstSubscriber, msg)
		msg.Ack()
	}
	requirePublished(t, published)

	tests.AssertAllMessagesReceived(t, messagesToPublish, receivedByFirstSubscriber)
	select {
	case received := <-receivedByNewSubscriber:
		tests.AssertAllMessagesReceived(t, messagesToPublish, received)
	case <-time.After(deadlockTimeout):
		t.Fatalf("new subscriber didn't receive the messages within %s", deadlockTimeout)
	}
}

func TestPublishSubscribe_block_until_ack_close_unblocks_publish(t *testing.T) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: true},
		watermill.NewStdLogger(true, true),
	)
	topicName := "test_topic_" + watermill.NewUUID()

	messages := subscribeWithTimeout(t, pubSub, topicName)

	published := publishAsync(pubSub, topicName, message.NewMessage("1", nil))
	_ = receiveWithTimeout(t, messages) // never acked

	require.NoError(t, pubSub.Close())

	requirePublished(t, published)
}

// deadlockTimeout bounds waiting for operations which should return almost immediately,
// so a deadlock fails the test quickly instead of hanging it.
const deadlockTimeout = 500 * time.Millisecond

func publishAsync(publisher message.Publisher, topic string, messages ...*message.Message) <-chan error {
	published := make(chan error, 1)
	go func() {
		published <- publisher.Publish(topic, messages...)
	}()
	return published
}

func requirePublished(t *testing.T, published <-chan error) {
	t.Helper()

	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(deadlockTimeout):
		t.Fatalf("Publish didn't return within %s", deadlockTimeout)
	}
}

func subscribeWithTimeout(t *testing.T, sub message.Subscriber, topic string) <-chan *message.Message {
	t.Helper()

	type subscribeResult struct {
		messages <-chan *message.Message
		err      error
	}
	subscribed := make(chan subscribeResult, 1)
	go func() {
		messages, err := sub.Subscribe(context.Background(), topic)
		subscribed <- subscribeResult{messages: messages, err: err}
	}()

	select {
	case result := <-subscribed:
		require.NoError(t, result.err)
		return result.messages
	case <-time.After(deadlockTimeout):
		t.Fatalf("Subscribe didn't return within %s", deadlockTimeout)
		return nil
	}
}

func receiveWithTimeout(t *testing.T, messages <-chan *message.Message) *message.Message {
	t.Helper()

	select {
	case msg, ok := <-messages:
		require.True(t, ok, "messages channel closed")
		return msg
	case <-time.After(deadlockTimeout):
		t.Fatalf("no message received within %s", deadlockTimeout)
		return nil
	}
}

// requireNoMessage can't prove that no message comes later,
// but a message which shouldn't be sent yet usually comes right away.
func requireNoMessage(t *testing.T, messages <-chan *message.Message) {
	t.Helper()

	select {
	case msg := <-messages:
		t.Fatalf("unexpected message %s", msg.UUID)
	case <-time.After(50 * time.Millisecond):
		// ok
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
