package message

import (
	"sync"
	"github.com/renstrom/shortuuid"
)

// todo - use it factory only for new messages
func DefaultFactoryFunc(payload Payload) (*Message, error) {
	return &Message{
		UUID:         shortuuid.New(),
		Payload:      payload,
		ackListeners: []chan<- struct{}{},
		ackLock:      &sync.Mutex{},
	}, nil
}
