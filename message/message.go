package message

import (
	"sync"
)

var closedchan = make(chan struct{})

func init() {
	close(closedchan)
}

type Payload []byte

type Message struct {
	UUID string // todo - change to []byte?, change to type

	Metadata Metadata

	Payload Payload

	ack      chan struct{}
	noAck    chan struct{}
	ackMutex sync.Mutex
	ackSent  ackType
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

func (m *Message) Ack() {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSent == nack {
		panic("already Nacked")
	}
	if m.ackSent != noAckSent {
		return
	}

	m.ackSent = ack
	if m.noAck == nil {
		m.ack = closedchan
	} else {
		close(m.ack)
	}
}

func (m *Message) Nack() {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSent == ack {
		panic("already Acked")
	}
	if m.ackSent != noAckSent {
		return
	}

	m.ackSent = nack

	if m.noAck == nil {
		m.noAck = closedchan
	} else {
		close(m.noAck)
	}
}

func (m *Message) Acked() <-chan struct{} {
	return m.ack
}

func (m *Message) Nacked() <-chan struct{} {
	return m.noAck
}
