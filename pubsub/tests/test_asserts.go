package tests

import (
	"sort"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
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

func MissingMessages(expected message.Messages, received message.Messages) []string {
	sentIDs := expected.IDs()
	receivedIDs := received.IDs()

	sort.Strings(sentIDs)
	sort.Strings(receivedIDs)

	return difference(sentIDs, receivedIDs)
}

func AssertAllMessagesReceived(t *testing.T, sent message.Messages, received message.Messages) bool {
	sentIDs := sent.IDs()
	receivedIDs := received.IDs()

	sort.Strings(sentIDs)
	sort.Strings(receivedIDs)

	assert.Equal(
		t,
		len(sentIDs), len(receivedIDs),
		"id's count is different: received: %d, sent: %d", len(receivedIDs), len(sentIDs),
	)

	return assert.Equal(
		t, sentIDs, receivedIDs,
		"received different messages ID's, missing: %s, extra %s",
		MissingMessages(sent, received),
		MissingMessages(received, sent),
	)
}

func AssertMessagesPayloads(
	t *testing.T,
	expectedPayloads map[string]interface{},
	received []*message.Message,
) bool {
	assert.Len(t, received, len(expectedPayloads))

	receivedMsgs := map[string]interface{}{}
	for _, msg := range received {
		receivedMsgs[msg.UUID] = string(msg.Payload)
	}

	ok := true
	for msgUUID, sentMsgPayload := range expectedPayloads {
		if !assert.EqualValues(t, sentMsgPayload, receivedMsgs[msgUUID]) {
			ok = false
		}
	}

	return ok
}

func AssertMessagesMetadata(t *testing.T, key string, expectedValues map[string]string, received []*message.Message) bool {
	assert.Len(t, received, len(expectedValues))

	ok := true
	for _, msg := range received {
		if !assert.Equal(t, expectedValues[msg.UUID], msg.Metadata[key]) {
			ok = false
		}
	}

	return ok
}
