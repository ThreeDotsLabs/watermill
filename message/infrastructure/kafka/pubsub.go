package kafka

import (
	"github.com/roblaszczak/gooddd"
	"github.com/roblaszczak/gooddd/message"
	"github.com/pkg/errors"
)

func NewPubSub(brokers []string, mu MarshallerUnmarshaller, consumerGroup string, logger gooddd.LoggerAdapter) (message.PubSub, error) {
	publisher, err := NewPublisher(brokers, mu)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create publisher")
	}

	subscriber := NewConfluentSubscriber(brokers, consumerGroup, mu, logger)

	return message.NewPubSub(publisher, subscriber), nil
}
