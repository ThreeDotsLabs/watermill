package cli

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

type Message struct {
	// ID is a unique message ID across the Pub/Sub's topic.
	ID       string
	UUID     string
	Payload  string
	Metadata map[string]string

	OriginalTopic string
	DelayedUntil  string
	DelayedFor    string
	RequeueIn     time.Duration
}

func NewMessage(id string, uuid string, payload string, metadata map[string]string) (Message, error) {
	originalTopic := metadata[middleware.PoisonedTopicKey]

	// Calculate the time until the message should be requeued
	delayedUntil, err := time.Parse(time.RFC3339, metadata[delay.DelayedUntilKey])
	if err != nil {
		return Message{}, err
	}

	delayedFor := metadata[delay.DelayedForKey]
	requeueIn := delayedUntil.Sub(time.Now().UTC()).Round(time.Second)

	return Message{
		ID:            id,
		UUID:          uuid,
		Payload:       payload,
		Metadata:      metadata,
		OriginalTopic: originalTopic,
		DelayedUntil:  delayedUntil.String(),
		DelayedFor:    delayedFor,
		RequeueIn:     requeueIn,
	}, nil
}
