package forwarder

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

const defaultForwarderTopic = "forwarder_topic"

type Config struct {
	// ForwarderTopic is a topic on which the forwarder will be listening to enveloped messages to forward.
	// Defaults to `forwarder_topic`.
	ForwarderTopic string

	// Middlewares are used to decorate forwarder's handler function.
	Middlewares []message.HandlerMiddleware

	// CloseTimeout determines how long router should work for handlers when closing.
	CloseTimeout time.Duration

	// AckWhenCannotUnwrap enables acking of messages which cannot be unwrapped from an envelope.
	AckWhenCannotUnwrap bool
}

func (c *Config) setDefaults() {
	if c.CloseTimeout == 0 {
		c.CloseTimeout = time.Second * 30
	}
	if c.ForwarderTopic == "" {
		c.ForwarderTopic = defaultForwarderTopic
	}
}

func (c *Config) Validate() error {
	if c.ForwarderTopic == "" {
		return errors.New("empty forwarder topic")
	}

	return nil
}

// Forwarder subscribes to the topic provided in the config and publishes them to the destination topic embedded in the enveloped message.
type Forwarder struct {
	router    *message.Router
	publisher message.Publisher
	logger    watermill.LoggerAdapter
	config    Config
}

// NewForwarder creates a forwarder which will subscribe to the topic provided in the config using the provided subscriber.
// It will publish messages received on this subscription to the destination topic embedded in the enveloped message using the provided publisher.
//
// Provided subscriber and publisher can be from different Watermill Pub/Sub implementations, i.e. MySQL subscriber and Google Pub/Sub publisher.
//
// Note: Keep in mind that by default the forwarder will nack all messages which weren't sent using a decorated publisher.
// You can change this behavior by passing a middleware which will ack them instead.
func NewForwarder(subscriberIn message.Subscriber, publisherOut message.Publisher, logger watermill.LoggerAdapter, config Config) (*Forwarder, error) {
	config.setDefaults()

	routerConfig := message.RouterConfig{CloseTimeout: config.CloseTimeout}
	if err := routerConfig.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid router config")
	}

	router, err := message.NewRouter(routerConfig, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create a router")
	}

	f := &Forwarder{router, publisherOut, logger, config}

	router.AddNoPublisherHandler(
		"events_forwarder",
		config.ForwarderTopic,
		subscriberIn,
		f.forwardMessage,
	)

	router.AddMiddleware(config.Middlewares...)

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

func (f *Forwarder) forwardMessage(msg *message.Message) error {
	destTopic, unwrappedMsg, err := unwrapMessageFromEnvelope(msg)
	if err != nil {
		f.logger.Error("Could not unwrap a message from an envelope", err, watermill.LogFields{
			"uuid":     msg.UUID,
			"payload":  msg.Payload,
			"metadata": msg.Metadata,
			"acked":    f.config.AckWhenCannotUnwrap,
		})

		if f.config.AckWhenCannotUnwrap {
			return nil
		}
		return errors.Wrap(err, "cannot unwrap message from an envelope")
	}

	if err := f.publisher.Publish(destTopic, unwrappedMsg); err != nil {
		return errors.Wrap(err, "cannot publish a message")
	}

	return nil
}
