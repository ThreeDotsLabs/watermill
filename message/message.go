package message

import (
	"context"
	"sync"
)

type Payload interface{}

// todo - encapsulate fields?
// todo - replace with interface?
type Message struct {
	Payload Payload

	Metadata struct {
		// todo - fill it
		OriginService string
		OriginHost    string
		CorrelationID string
		// todo - add something to help with tracing? hosts list?
	}

	Context context.Context

	ackListeners []chan<- struct{}
	acked        bool
	ackLock      sync.Locker
}

// todo - remove ptr
func (m *Message) Acknowledged() (<-chan struct{}) {
	m.ackLock.Lock()
	defer m.ackLock.Unlock()
	ch := make(chan struct{}, 1)

	if m.acked {
		ch <- struct{}{}
		return ch
	}
	m.ackListeners = append(m.ackListeners, ch)

	return ch
}

func (m *Message) Acknowledge() {
	m.ackLock.Lock()
	defer m.ackLock.Unlock()

	if m.acked {
		return
	}
	m.acked = true

	for _, ch := range m.ackListeners {
		ch <- struct{}{}
	}
}
