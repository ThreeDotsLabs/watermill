package message

import "sync"

func DefaultFactoryFunc(payload Payload) (*Message, error) {
	return &Message{
		Payload:      payload,
		ackListeners: []chan<- struct{}{},
		ackLock:      &sync.Mutex{},
	}, nil
}
