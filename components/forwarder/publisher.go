package forwarder

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type PublisherConfig struct {
	// ForwarderTopic is a topic which the forwarder is listening to. Publisher will send enveloped messages to this topic.
	// Defaults to `forwarder_topic`.
	ForwarderTopic string
}

func (c *PublisherConfig) setDefaults() {
	if c.ForwarderTopic == "" {
		c.ForwarderTopic = defaultForwarderTopic
	}
}

func (c *PublisherConfig) Validate() error {
	if c.ForwarderTopic == "" {
		return errors.New("empty forwarder topic")
	}

	return nil
}

// PublisherDecorator changes `Publish` method behavior so it wraps a sent message in an envelope
// and sends it to the forwarder topic provided in the config.
type PublisherDecorator struct {
	wrappedPublisher message.Publisher
	config           PublisherConfig
}

func NewPublisherDecorator(publisher message.Publisher, config PublisherConfig) *PublisherDecorator {
	config.setDefaults()

	return &PublisherDecorator{
		wrappedPublisher: publisher,
		config:           config,
	}
}

func (p *PublisherDecorator) Publish(topic string, messages ...*message.Message) error {
	envelopedMessages := make([]*message.Message, 0, len(messages))
	for _, msg := range messages {
		envelopedMsg, err := wrapMessageInEnvelope(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot wrap message, target topic: '%s', uuid: '%s'", topic, msg.UUID)
		}

		envelopedMessages = append(envelopedMessages, envelopedMsg)
	}

	if err := p.wrappedPublisher.Publish(p.config.ForwarderTopic, envelopedMessages...); err != nil {
		return errors.Wrapf(err, "cannot publish messages to forwarder topic: '%s'", p.config.ForwarderTopic)
	}

	return nil
}

func (p *PublisherDecorator) Close() error {
	return p.wrappedPublisher.Close()
}
