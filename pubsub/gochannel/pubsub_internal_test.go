package gochannel

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestSubscribe_clean_subscriber_data(t *testing.T) {
	subCount := 100
	pubSub := NewGoChannel(
		Config{OutputChannelBuffer: int64(subCount)},
		watermill.NewStdLogger(false, false),
	)
	topicName := "test_topic"

	for i := 0; i < subCount; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		_, err := pubSub.Subscribe(ctx, topicName+"_index_"+strconv.Itoa(i))
		require.NoError(t, err)
		cancel()
	}

	err := pubSub.Close()
	require.NoError(t, err)

	assert.Len(t, pubSub.subscribers, 0)
	lockCount := 0
	pubSub.subscribersByTopicLock.Range(func(_, _ any) bool {
		lockCount++
		return true
	})
	assert.Equal(t, 0, lockCount)

	assert.NoError(t, pubSub.Close())
}

func TestPublish_clean_lock_data(t *testing.T) {
	messageCount := 100
	pubSub := NewGoChannel(
		Config{OutputChannelBuffer: int64(messageCount)},
		watermill.NewStdLogger(false, false),
	)
	topicName := "test_topic"

	_, err := pubSub.Subscribe(context.Background(), topicName+"_index_"+strconv.Itoa(0))
	require.NoError(t, err)

	for i := 0; i < messageCount; i++ {
		err := pubSub.Publish(topicName+"_index_"+strconv.Itoa(i), message.NewMessage(watermill.NewShortUUID(), nil))
		require.NoError(t, err)
	}

	lockCount := 0
	pubSub.subscribersByTopicLock.Range(func(_, _ any) bool {
		lockCount++
		return true
	})
	assert.Equal(t, 1, lockCount)

	assert.NoError(t, pubSub.Close())
}

// TestPublish_concurrent_close ensures Close does not race with in-flight persistent Publish calls.
func TestPublish_concurrent_close(t *testing.T) {
	pubSub := NewGoChannel(
		Config{Persistent: true},
		watermill.NewStdLogger(false, false),
	)
	topicName := "test_topic"

	publisherCount := 20
	startPublishing := make(chan struct{})
	wg := sync.WaitGroup{}

	for i := 0; i < publisherCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startPublishing
			for {
				err := pubSub.Publish(topicName, message.NewMessage(watermill.NewShortUUID(), nil))
				if err != nil {
					return
				}
			}
		}()
	}

	close(startPublishing)
	require.NoError(t, pubSub.Close())
	wg.Wait()
}

// Guards the deadlock where Publish held subscribersLock while waiting for the ack: removing the other
// subscriber queued for that lock, which blocked the handler's publish to another topic before acking
// (what the Router does), so the message was never acked.
func TestPublish_block_until_ack_unsubscribe_while_waiting_for_ack(t *testing.T) {
	pubSub := NewGoChannel(
		Config{BlockPublishUntilSubscriberAck: true},
		watermill.NewStdLogger(false, false),
	)
	defer func() { assert.NoError(t, pubSub.Close()) }()

	messages, err := pubSub.Subscribe(context.Background(), "topic_a")
	require.NoError(t, err)

	otherSubscriberCtx, cancelOtherSubscriber := context.WithCancel(context.Background())
	_, err = pubSub.Subscribe(otherSubscriberCtx, "topic_a")
	require.NoError(t, err)

	published := make(chan error, 1)
	go func() {
		published <- pubSub.Publish("topic_a", message.NewMessage("1", nil))
	}()

	var msg *message.Message
	select {
	case msg = <-messages:
	case <-time.After(deadlockTimeout):
		t.Fatal("message not received")
	}

	cancelOtherSubscriber()
	waitWithTimeout(t, "removing the other subscriber", func() {
		for {
			pubSub.subscribersLock.RLock()
			subscribersCount := len(pubSub.subscribers["topic_a"])
			pubSub.subscribersLock.RUnlock()

			if subscribersCount == 1 {
				return
			}
			time.Sleep(time.Millisecond)
		}
	})

	waitWithTimeout(t, "publishing to another topic", func() {
		assert.NoError(t, pubSub.Publish("topic_b", message.NewMessage("2", nil)))
	})
	msg.Ack()

	waitWithTimeout(t, "publishing the acked message", func() {
		assert.NoError(t, <-published)
	})
}

func TestPublish_block_until_ack_clean_lock_data(t *testing.T) {
	messageCount := 100
	pubSub := NewGoChannel(
		Config{BlockPublishUntilSubscriberAck: true},
		watermill.NewStdLogger(false, false),
	)
	defer func() { assert.NoError(t, pubSub.Close()) }()

	messages, err := pubSub.Subscribe(context.Background(), "topic_with_subscriber")
	require.NoError(t, err)
	go func() {
		for msg := range messages {
			msg.Ack()
		}
	}()

	for i := 0; i < messageCount; i++ {
		require.NoError(t, pubSub.Publish("topic_"+strconv.Itoa(i), message.NewMessage(watermill.NewShortUUID(), nil)))
		require.NoError(t, pubSub.Publish("topic_with_subscriber", message.NewMessage(watermill.NewShortUUID(), nil)))
	}

	pubSub.blockingPublishLocks.lock.Lock()
	defer pubSub.blockingPublishLocks.lock.Unlock()
	assert.Empty(t, pubSub.blockingPublishLocks.locks)
}

// deadlockTimeout bounds waiting for operations which should return almost immediately,
// so a deadlock fails the test quickly instead of hanging it.
const deadlockTimeout = 500 * time.Millisecond

func waitWithTimeout(t *testing.T, operation string, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(deadlockTimeout):
		t.Fatalf("%s didn't finish within %s", operation, deadlockTimeout)
	}
}
