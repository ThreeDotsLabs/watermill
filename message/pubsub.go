package message

import (
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

type PubSub interface {
	publisher
	subscriber

	Close() error
}

func NewPubSub(publisher Publisher, subscriber Subscriber) PubSub {
	return pubSub{publisher, subscriber}
}

type pubSub struct {
	pub Publisher
	sub Subscriber
}

func (p pubSub) Publish(topic string, messages ...*Message) error {
	return p.pub.Publish(topic, messages...)
}

func (p pubSub) Subscribe(topic string) (chan *Message, error) {
	return p.sub.Subscribe(topic)
}

func (p pubSub) Publisher() Publisher {
	return p.pub
}

func (p pubSub) Subscriber() Subscriber {
	return p.sub
}

func (p pubSub) Close() error {
	var err error

	if publisherErr := p.pub.Close(); publisherErr != nil {
		err = multierror.Append(err, errors.Wrap(publisherErr, "cannot close publisher"))
	}
	if subscriberErr := p.sub.Close(); subscriberErr != nil {
		err = multierror.Append(err, errors.Wrap(subscriberErr, "cannot close subscriber"))
	}

	return err
}
