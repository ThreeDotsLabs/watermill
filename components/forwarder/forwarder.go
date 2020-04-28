package forwarder

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// Forwarder subscribes to the topic provided in the config and publishes them to the destination topic embedded in the enveloped message.
type Forwarder struct {
	router    *message.Router
	publisher message.Publisher
	config    Config
}

// NewForwarder creates a forwarder which will subscribe to the topic provided in the provided config using the provided subscriber.
// It will publish messages received on this subscription to the destination topic embedded in the enveloped message using the provided publisher.
//
// Provided subscriber and publisher can be from different Watermill Pub/Sub implementations, i.e. MySQL subscriber and Google Pub/Sub publisher.
func NewForwarder(subscriberIn message.Subscriber, publisherOut message.Publisher, logger watermill.LoggerAdapter, config Config) (*Forwarder, error) {
	config.setDefaults()

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	f := &Forwarder{router, publisherOut, config}

	router.AddNoPublisherHandler(
		"events_forwarder",
		config.ForwarderTopic,
		subscriberIn,
		f.forwardMessage,
	)

	return f, nil
}

// Run runs forwarder's handler responsible for forwarding messages.
// This call is blocking while the forwarder is running.
// ctx will be propagated to the forwarder's subscription.
//
// To stop Run() you should call Close() on the forwarder.
func (f *Forwarder) Run(ctx context.Context) error {
	return f.router.Run(ctx)
}

// Close stops forwarder's handler.
func (f *Forwarder) Close() error {
	return f.router.Close()
}

// Running returns channel which is closed when the forwarder is running.
func (f *Forwarder) Running() chan struct{} {
	return f.router.Running()
}

// DecoratePublisher is a helper method which decorates a given publisher so when it publishes messages,
// it envelopes them and publish directly to the forwarder's topic provided in the forwarder's config.
//
// Enveloping means that the message is put into the generic envelope containing the original message
// and a destination topic taken from publisher `Publish` method. The destination topic is used later
// by the forwarder to forward it to a specific topic.
func (f *Forwarder) DecoratePublisher(publisher message.Publisher) message.Publisher {
	return NewPublisherDecorator(publisher, f.config)
}

func (f *Forwarder) forwardMessage(msg *message.Message) error {
	destTopic, unwrappedMsg, err := unwrapMessageFromEnvelope(msg)
	if err != nil {
		return errors.Wrap(err, "cannot unwrap message from envelope")
	}

	if err := f.publisher.Publish(destTopic, unwrappedMsg); err != nil {
		return errors.Wrap(err, "cannot publish message")
	}

	return nil
}
