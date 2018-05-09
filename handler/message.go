package handler

import (
	"context"
)

type MessagePayload interface{}

// todo - distingush output from input msg??
type Message struct {
	Payload MessagePayload

	Metadata struct {
		// todo - fill it
		OriginService string
		OriginHost    string
		CorrelationID string
		// todo - add something to help with tracing? hosts list?
	}

	context context.Context
}
