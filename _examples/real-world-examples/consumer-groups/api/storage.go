package main

import (
	"sync"

	"github.com/ThreeDotsLabs/watermill-routing-example/server/common"
)

type storage struct {
	lock             *sync.Mutex
	receivedMessages map[string][]common.MessageReceived
}

func (s *storage) Append(message common.MessageReceived) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for k, v := range s.receivedMessages {
		s.receivedMessages[k] = append(v, message)
	}
}

func (s *storage) PopAll(key string) []common.MessageReceived {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.receivedMessages[key]; !ok {
		s.receivedMessages[key] = []common.MessageReceived{}
		return []common.MessageReceived{}
	}

	messages := s.receivedMessages[key]
	s.receivedMessages[key] = []common.MessageReceived{}
	return messages
}
