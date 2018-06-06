package message

import "sync"

type Ack struct {
	ackListeners []chan<- struct{} `json:"-"`
	acked        bool              `json:"-"`
	ackLock      sync.Locker       `json:"-"`
}

func NewAck() *Ack {
	return &Ack{make([]chan<- struct{}, 0), false, &sync.Mutex{}}
}

func (m *Ack) Acknowledged() (<-chan struct{}) {
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

func (m *Ack) Acknowledge() {
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
