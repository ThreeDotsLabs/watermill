package message

import "github.com/pkg/errors"

// todo - pass pubsub only where all methods are needed
type PubSub interface {
	Publisher
	Subscriber

	Close() error
}

type pubSub struct {
	Publisher
	Subscriber
}

func (p pubSub) Close() error {
	publisherErr := p.Publisher.ClosePublisher()
	subscriberErr := p.Subscriber.CloseSubscriber()

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

func NewPubSub(publisher Publisher, subscriber Subscriber) PubSub {
	return pubSub{publisher, subscriber}
}
