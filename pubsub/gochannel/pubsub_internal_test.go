package gochannel

import (
	"context"
	"log"
	"strconv"
	"sync"
	"testing"

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

	allClosed := sync.WaitGroup{}

	for i := 0; i < subCount; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		_, err := pubSub.Subscribe(ctx, topicName+"_index_"+strconv.Itoa(i))
		require.NoError(t, err)

		allClosed.Add(1)
		go func() {
			cancel()
			allClosed.Done()
		}()
	}

	log.Println("waiting for all closed")
	allClosed.Wait()

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
