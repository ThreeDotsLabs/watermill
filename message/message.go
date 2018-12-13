package message

import (
	"sync"

	"github.com/pkg/errors"
)

var closedchan = make(chan struct{})

func init() {
	close(closedchan)
}

var (
	ErrAlreadyAcked  = errors.New("message already acked")
	ErrAlreadyNacked = errors.New("message already nacked")
)

type Payload []byte

type Message struct {
	// UUID is an unique identifier of message.
	//
	// It is only used by Watermill for debugging.
	// UUID can be empty.
	UUID string

	// Metadata contains the message metadata.
	//
	// Can be used to store data which doesn't require unmarshaling entire payload.
	// It is something similar to HTTP request's headers.
	Metadata Metadata

	// Payload is message's payload.
	Payload Payload

	// ack is closed, when acknowledge is received.
	ack chan struct{}
	// noACk is closed, when negative acknowledge is received.
	noAck chan struct{}

	ackMutex    sync.Mutex
	ackSentType ackType
}

func NewMessage(uuid string, payload Payload) *Message {
	return &Message{
		UUID:     uuid,
		Metadata: make(map[string]string),
		Payload:  payload,
		ack:      make(chan struct{}),
		noAck:    make(chan struct{}),
	}
}

type ackType int

const (
	noAckSent ackType = iota
	ack
	nack
)

// Ack sends message's acknowledgement.
//
// Ack is not blocking.
// Ack is idempotent.
// Error is returned, if Nack is already sent.
func (m *Message) Ack() error {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSentType == nack {
		return ErrAlreadyNacked
	}
	if m.ackSentType != noAckSent {
		return nil
	}

	m.ackSentType = ack
	if m.noAck == nil {
		m.ack = closedchan
	} else {
		close(m.ack)
	}

	return nil
}

// Nack sends message's negative acknowledgement.
//
// Nack is not blocking.
// Nack is idempotent.
// Error is returned, if Ack is already sent.
func (m *Message) Nack() error {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSentType == ack {
		return ErrAlreadyAcked
	}
	if m.ackSentType != noAckSent {
		return nil
	}

	m.ackSentType = nack

	if m.noAck == nil {
		m.noAck = closedchan
	} else {
		close(m.noAck)
	}

	return nil
}

// Acked returns channel which is closed when acknowledgement is sent.
//
// Usage:
// 		select {
//		case <-message.Acked():
// 			// ack received
//		case <-message.Nacked():
//			// nack received
//		}
func (m *Message) Acked() <-chan struct{} {
	return m.ack
}

// Nacked returns channel which is closed when negative acknowledgement is sent.
//
// Usage:
// 		select {
//		case <-message.Acked():
// 			// ack received
//		case <-message.Nacked():
//			// nack received
//		}
func (m *Message) Nacked() <-chan struct{} {
	return m.noAck
}
