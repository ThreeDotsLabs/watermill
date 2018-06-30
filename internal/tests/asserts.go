package tests

import (
	"github.com/roblaszczak/gooddd/message"
	"github.com/stretchr/testify/assert"
	"testing"
	"sort"
	"fmt"
)

func difference(a, b []string) []string {
	mb := map[string]bool{}
	for _, x := range b {
		mb[x] = true
	}
	ab := []string{}
	for _, x := range a {
		if _, ok := mb[x]; !ok {
			ab = append(ab, x)
		}
	}
	return ab
}

func MissingMessages(expected, received []message.Message) []string {
	sentIDs := messagesIDs(expected)
	receivedIDs := messagesIDs(received)

	sort.Strings(sentIDs)
	sort.Strings(receivedIDs)

	return difference(sentIDs, receivedIDs)
}

func messagesIDs(messages []message.Message) []string {
	var ids []string
	for _, msg := range messages {
		ids = append(ids, msg.UUID())
	}

	return ids
}

func AssertAllMessagesReceived(t *testing.T, sent, received []message.Message) bool {
	sentIDs := messagesIDs(sent)
	receivedIDs := messagesIDs(received)

	sort.Strings(sentIDs)
	sort.Strings(receivedIDs)

	fmt.Println(difference(sentIDs, receivedIDs))

	return assert.EqualValues(t, receivedIDs, sentIDs)
}

func AssertMessagesPayloads(
	t *testing.T,
	expectedPayloads map[string]interface{},
	received []message.Message,
	unmarshalMsg func(msg message.Message) interface{},
) bool {
	assert.Len(t, received, len(expectedPayloads))

	receivedMsgs := map[string]interface{}{}
	for _, msg := range received {
		payload := unmarshalMsg(msg)
		receivedMsgs[msg.UUID()] = payload
	}

	ok := true
	for msgUUID, sentMsgPayload := range expectedPayloads {
		if !assert.EqualValues(t, sentMsgPayload, receivedMsgs[msgUUID]) {
			ok = false
		}
	}

	return ok
}

func AssertMessagesMetadata(t *testing.T, key string, expectedValues map[string]string, received []message.Message) bool {
	assert.Len(t, received, len(expectedValues))

	ok := true
	for _, msg := range received {
		if !assert.Equal(t, expectedValues[msg.UUID()], msg.GetMetadata(key)) {
			ok = false
		}
	}

	return ok
}
