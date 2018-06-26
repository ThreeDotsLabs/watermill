package kafka

import (
	"github.com/roblaszczak/gooddd/message"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd"
)

type pubsub struct {
	message.Publisher
	message.Subscriber
}

func (p pubsub) Close() error {
	publisherErr := p.Publisher.ClosePublisher()
	subscriberErr := p.Subscriber.CloseSubscriber()

	if publisherErr == nil && subscriberErr == nil {
		return nil
	}

	errMsg := "cannot close pubsub: "
	if publisherErr != nil {
		errMsg += "publisher err: " + publisherErr.Error()
	}
	if subscriberErr != nil {
		errMsg += "subscriber err: " + subscriberErr.Error()
	}

	return errors.New(errMsg)
}

func NewPubSub(brokers []string, mu MarshallerUnmarshaller, consumerGroup string, logger gooddd.LoggerAdapter) (message.PubSub, error) {
	publisher, err := NewPublisher(brokers, mu)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create publisher")
	}

	subscriber := NewSubscriber(brokers, consumerGroup, mu, logger)

	return pubsub{publisher, subscriber}, nil
}
