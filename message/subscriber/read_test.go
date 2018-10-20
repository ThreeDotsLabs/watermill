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

type mockConsumedMessage struct {
	message.Message
}

func (mockConsumedMessage) SetMetadata(key, value string) {
	panic("not implemented")
}

func (mockConsumedMessage) Payload() message.Payload {
	panic("not implemented")
}

func (mockConsumedMessage) UnmarshalPayload(val interface{}) error {
	panic("not implemented")
}

func (mockConsumedMessage) Acknowledged() (<-chan error) {
	panic("not implemented")
}

func (mockConsumedMessage) Acknowledge() error {
	return nil
}

func (mockConsumedMessage) Error(err error) error {
	panic("not implemented")
}

func TestBulkRead(t *testing.T) {
	messagesCount := 100

	var messages []message.ProducedMessage
	messagesCh := make(chan message.ConsumedMessage, messagesCount)

	for i := 0; i < messagesCount; i++ {
		msg := mockConsumedMessage{message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})}

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

	var messages []message.ProducedMessage
	messagesCh := make(chan message.ConsumedMessage, messagesCount)

	for i := 0; i < messagesCount; i++ {
		msg := mockConsumedMessage{message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})}

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

	var messages []message.ProducedMessage
	messagesCh := make(chan message.ConsumedMessage, messagesCount)

	for i := 0; i < messagesCount; i++ {
		msg := mockConsumedMessage{message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})}

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

	var messages []message.ProducedMessage
	messagesCh := make(chan message.ConsumedMessage, messagesCount)
	messagesChClosed := false

	for i := 0; i < messagesCount; i++ {
		msg :=  mockConsumedMessage{message.NewDefault(uuid.NewV4().String(), SimpleMessage{i})}
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
