package message

import "github.com/pkg/errors"

type PubSub interface {
	publisher
	subscriber

	Close() error
}

type pubSub struct {
	Publisher
	Subscriber
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

func NewPubSub(publisher Publisher, subscriber Subscriber) PubSub {
	return pubSub{publisher, subscriber}
}
