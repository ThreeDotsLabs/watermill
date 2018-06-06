package message

import "sync"

type Ack struct {
	ackListeners []chan<- error `json:"-"`
	acked        bool           `json:"-"`
	ackLock      sync.Locker    `json:"-"`
}

func NewAck() *Ack {
	return &Ack{make([]chan<- error, 0), false, &sync.Mutex{}}
}

// todo - rename?
func (m *Ack) Acknowledged() (<-chan error) {
	m.ackLock.Lock()
	defer m.ackLock.Unlock()
	ch := make(chan error, 1)

	if m.acked {
		ch <- nil
		return ch
	}
	m.ackListeners = append(m.ackListeners, ch)

	return ch
}

func (m *Ack) sendAck(err error) {
	m.ackLock.Lock()
	defer m.ackLock.Unlock()

	if m.acked {
		// todo - test
		return
	}
	m.acked = true

	for _, ch := range m.ackListeners {
		ch <- err
	}
}

func (m *Ack) Acknowledge() {
	m.sendAck(nil)
}

// todo - rename?
func (m *Ack) Error(err error) {
	m.sendAck(err)

}
