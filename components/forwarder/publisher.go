package forwarder

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// PublisherConfig configures the decorating Publisher returned by NewPublisher.
type PublisherConfig struct {
	// ForwarderTopic is a topic which the forwarder is listening to. Publisher will send enveloped messages to this topic.
	// Defaults to `forwarder_topic`.
	ForwarderTopic string

	// Marshaler is used to serialize envelopes sent to ForwarderTopic.
	// It must match the Marshaler used by the Forwarder consuming the topic.
	// Defaults to DefaultMarshaler, which uses encoding/json.
	Marshaler Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.ForwarderTopic == "" {
		c.ForwarderTopic = defaultForwarderTopic
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
}

func (c *PublisherConfig) Validate() error {
	if c.ForwarderTopic == "" {
		return errors.New("empty forwarder topic")
	}

	return nil
}

// Publisher changes `Publish` method behavior so it wraps a sent message in an envelope
// and sends it to the forwarder topic provided in the config.
type Publisher struct {
	wrappedPublisher message.Publisher
	config           PublisherConfig
}

func NewPublisher(publisher message.Publisher, config PublisherConfig) *Publisher {
	config.setDefaults()

	return &Publisher{
		wrappedPublisher: publisher,
		config:           config,
	}
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	envelopedMessages := make([]*message.Message, 0, len(messages))
	for _, msg := range messages {
		envelopedMsg, err := wrapMessageInEnvelope(topic, msg, p.config.Marshaler)
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

func (p *Publisher) Close() error {
	return p.wrappedPublisher.Close()
}
