package message

import "github.com/pkg/errors"

type PubSub interface {
	publisher
	subscriber

	Close() error
}

func NewPubSub(publisher Publisher, subscriber Subscriber) PubSub {
	subInit, _ := subscriber.(SubscribeInitializer)

	return pubSub{publisher, subscriber, subInit}
}

type pubSub struct {
	Publisher
	Subscriber
	SubscribeInitializer
}

func (p pubSub) Close() error {
	publisherErr := p.Publisher.Close()
	subscriberErr := p.Subscriber.Close()

	if publisherErr == nil && subscriberErr == nil {
		return nil
	}

	errMsg := "cannot close pubSub: "
	if publisherErr != nil {
		errMsg += "publisher err: " + publisherErr.Error()
	}
	if subscriberErr != nil {
		errMsg += "subscriber err: " + subscriberErr.Error()
	}

	return errors.New(errMsg)
}
