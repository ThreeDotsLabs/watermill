package message

import (
	"context"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

type publisher interface {
	// Publish publishes provided messages to given topic.
	//
	// Publish can be synchronous or asynchronous - it depends of implementation.
	//
	// Most publishers implementations doesn't support atomic publishing of messages.
	// That means, that when publishing one of messages failed next messages will be not published.
	//
	// Publish must be thread safe.
	Publish(topic string, messages ...*Message) error
}

type Publisher interface {
	publisher

	// Close should flush unsent messages, if publisher is async.
	Close() error
}

type subscriber interface {
	// Subscribe returns output channel with messages from provided topic.
	// Channel is closed, when Close() was called to the subscriber.
	//
	// To receive next message, `Ack()` must be called on the received message.
	// If message processing was failed and message should be redelivered `Nack()` should be called.
	//
	// When provided ctx is cancelled, subscriber will close subscribe and close output channel.
	// Provided ctx is set to all produced messages.
	// When Nack or Ack is called on the message, context of the message is canceled.
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
}

type Subscriber interface {
	subscriber

	// Close closes all subscriptions with their output channels and flush offsets etc. when needed.
	Close() error
}

type SubscribeInitializer interface {
	// SubscribeInitialize can be called to initialize subscribe before consume.
	// When calling Subscribe before Publish, SubscribeInitialize should be not required.
	//
	// Not every Pub/Sub requires this initialize and it may be optional for performance improvements etc.
	// For detailed SubscribeInitialize functionality, please check Pub/Subs godoc.
	//
	// Implementing SubscribeInitialize is not obligatory.
	SubscribeInitialize(topic string) error
}

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

func (p pubSub) Subscribe(ctx context.Context, topic string) (<-chan *Message, error) {
	return p.sub.Subscribe(ctx, topic)
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
