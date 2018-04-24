package eventstore

import (
	"github.com/roblaszczak/gooddd/domain"
	"github.com/roblaszczak/gooddd/pubsub"
)

type pubSub struct {
	topic  string
	pubsub pubsub.PubSub
}

// todo -rename?
func NewPubSub(topic string, pubsub pubsub.PubSub) domain.Eventstore {
	return pubSub{topic, pubsub}
}

// todo - test
func (p pubSub) Save(events []domain.EventPayload) error {
	// todo
	p.pubsub.Publish(p.topic, domain.EventsToMessagePayloads(events)...)
	return nil
}
