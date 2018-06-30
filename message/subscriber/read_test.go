package subscriber_test

import (
	"testing"
	"github.com/roblaszczak/gooddd/message"
	"github.com/satori/go.uuid"
	"github.com/roblaszczak/gooddd/message/subscriber"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/roblaszczak/gooddd/internal/tests"
)

type SimpleMessage struct {
	Num int `json:"num"`
}

func TestBulkRead(t *testing.T) {
	messagesCount := 100

	var messages []message.Message
	messagesCh := make(chan message.Message, messagesCount)

	for i := 0; i < messagesCount; i++ {
		msg := message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})

		messages = append(messages, msg)
		messagesCh <- msg
	}

	readMessages, all := subscriber.BulkRead(messagesCh, messagesCount, time.Second)
	assert.True(t, all)

	tests.AssertAllMessagesReceived(t, messages, readMessages)
}

func TestBulkRead_timeout(t *testing.T) {
	messagesCount := 100
	sendLimit := 90

	var messages []message.Message
	messagesCh := make(chan message.Message, messagesCount)

	for i := 0; i < messagesCount; i++ {
		msg := message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})

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
}

func TestBulkRead_with_limit(t *testing.T) {
	messagesCount := 110
	limit := 100

	var messages []message.Message
	messagesCh := make(chan message.Message, messagesCount)

	for i := 0; i < messagesCount; i++ {
		msg := message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})

		messages = append(messages, msg)
		messagesCh <- msg
	}

	readMessages, all := subscriber.BulkRead(messagesCh, limit, time.Second)
	assert.True(t, all)
	assert.Equal(t, limit, len(readMessages))
}

func TestBulkRead_return_on_channel_close(t *testing.T) {
	messagesCount := 100
	sendLimit := 90

	var messages []message.Message
	messagesCh := make(chan message.Message, messagesCount)
	messagesChClosed := false

	for i := 0; i < messagesCount; i++ {
		msg := message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})
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
}
