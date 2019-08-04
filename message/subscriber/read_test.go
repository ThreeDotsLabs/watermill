package subscriber_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/stretchr/testify/assert"
)

type bulkReadFunc func(messagesCh <-chan *message.Message, limit int, timeout time.Duration) (receivedMessages message.Messages, all bool)

func TestBulkRead(t *testing.T) {
	testCases := []struct {
		Name         string
		BulkReadFunc bulkReadFunc
	}{
		{
			Name:         "BulkRead",
			BulkReadFunc: subscriber.BulkRead,
		},
		{
			Name:         "BulkReadWithDeduplication",
			BulkReadFunc: subscriber.BulkReadWithDeduplication,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			messagesCount := 100

			var messages []*message.Message
			messagesCh := make(chan *message.Message, messagesCount)

			for i := 0; i < messagesCount; i++ {
				msg := message.NewMessage(watermill.NewUUID(), nil)

				messages = append(messages, msg)
				messagesCh <- msg
			}

			readMessages, all := subscriber.BulkRead(messagesCh, messagesCount, time.Second)
			assert.True(t, all)

			tests.AssertAllMessagesReceived(t, messages, readMessages)
		})
	}
}

func TestBulkRead_timeout(t *testing.T) {
	testCases := []struct {
		Name         string
		BulkReadFunc bulkReadFunc
	}{
		{
			Name:         "BulkRead",
			BulkReadFunc: subscriber.BulkRead,
		},
		{
			Name:         "BulkReadWithDeduplication",
			BulkReadFunc: subscriber.BulkReadWithDeduplication,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			messagesCount := 100
			sendLimit := 90

			var messages []*message.Message
			messagesCh := make(chan *message.Message, messagesCount)

			for i := 0; i < messagesCount; i++ {
				msg := message.NewMessage(watermill.NewUUID(), nil)

				messages = append(messages, msg)

				if i < sendLimit {
					messagesCh <- msg
				}
			}

			bulkReadStart := time.Now()
			readMessages, all := subscriber.BulkRead(messagesCh, messagesCount, time.Millisecond)

			assert.WithinDuration(t, bulkReadStart, time.Now(), time.Millisecond*100)
			assert.False(t, all)
			assert.Equal(t, sendLimit, len(readMessages))
		})
	}
}

func TestBulkRead_with_limit(t *testing.T) {
	testCases := []struct {
		Name         string
		BulkReadFunc bulkReadFunc
	}{
		{
			Name:         "BulkRead",
			BulkReadFunc: subscriber.BulkRead,
		},
		{
			Name:         "BulkReadWithDeduplication",
			BulkReadFunc: subscriber.BulkReadWithDeduplication,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			messagesCount := 110
			limit := 100

			var messages []*message.Message
			messagesCh := make(chan *message.Message, messagesCount)

			for i := 0; i < messagesCount; i++ {
				msg := message.NewMessage(watermill.NewUUID(), nil)

				messages = append(messages, msg)
				messagesCh <- msg
			}

			readMessages, all := subscriber.BulkRead(messagesCh, limit, time.Second)
			assert.True(t, all)
			assert.Equal(t, limit, len(readMessages))
		})
	}
}

func TestBulkRead_return_on_channel_close(t *testing.T) {
	testCases := []struct {
		Name         string
		BulkReadFunc bulkReadFunc
	}{
		{
			Name:         "BulkRead",
			BulkReadFunc: subscriber.BulkRead,
		},
		{
			Name:         "BulkReadWithDeduplication",
			BulkReadFunc: subscriber.BulkReadWithDeduplication,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			messagesCount := 100
			sendLimit := 90

			var messages []*message.Message
			messagesCh := make(chan *message.Message, messagesCount)
			messagesChClosed := false

			for i := 0; i < messagesCount; i++ {
				msg := message.NewMessage(watermill.NewUUID(), nil)
				messages = append(messages, msg)

				if i < sendLimit {
					messagesCh <- msg
				} else if !messagesChClosed {
					close(messagesCh)
					messagesChClosed = true
				}
			}

			bulkReadStart := time.Now()
			_, all := subscriber.BulkRead(messagesCh, messagesCount, time.Second)

			assert.WithinDuration(t, bulkReadStart, time.Now(), time.Millisecond*100)
			assert.False(t, all)
		})
	}
}

func TestBulkReadWithDeduplication(t *testing.T) {
	messagesCh := make(chan *message.Message, 3)

	msg1 := message.NewMessage(watermill.NewUUID(), nil)
	msg2 := message.NewMessage(watermill.NewUUID(), nil)
	messagesCh <- msg1
	messagesCh <- msg1
	messagesCh <- msg2

	readMessages, all := subscriber.BulkReadWithDeduplication(messagesCh, 2, time.Second)
	assert.True(t, all)

	assert.Equal(t, []string{msg1.UUID, msg2.UUID}, readMessages.IDs())
}
