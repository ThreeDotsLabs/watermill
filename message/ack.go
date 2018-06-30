package message

import (
	"sync"
	"github.com/pkg/errors"
)

type Ack struct {
	ackListeners []chan<- error
	ackLock      sync.Locker

	acked  bool
	ackErr error
}

func NewAck() *Ack {
	return &Ack{make([]chan<- error, 0),  &sync.Mutex{}, false, nil}
}

func (m *Ack) Acknowledged() (<-chan error) {
	m.ackLock.Lock()
	defer m.ackLock.Unlock()
	ch := make(chan error, 1)

	if m.acked {
		ch <- m.ackErr
		return ch
	}
	m.ackListeners = append(m.ackListeners, ch)

	return ch
}

func (m *Ack) sendAck(err error) error {
	m.ackLock.Lock()
	defer m.ackLock.Unlock()

	if m.acked {
		if m.ackErr != nil {
			return errors.New("error already sent")
		} else {
			return errors.New("ack already sent")
		}
	}
	m.acked = true
	m.ackErr = err

	for _, ch := range m.ackListeners {
		ch <- err
	}

	return nil
}

func (m *Ack) Acknowledge() error {
	return m.sendAck(nil)
}

func (m *Ack) Error(err error) error {
	return m.sendAck(err)
}
