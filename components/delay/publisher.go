package delay

import "github.com/ThreeDotsLabs/watermill/message"

// PublisherConfig is a configuration for the delay publisher.
type PublisherConfig struct {
	DefaultDelay Delay
}

// NewPublisher wraps a publisher with a delay mechanism.
// A message can be published with delay metadata set in the context by using the WithContext function.
// If the message doesn't have the delay metadata set, the default delay will be applied, if provided.
func NewPublisher(pub message.Publisher, config PublisherConfig) (message.Publisher, error) {
	return &publisher{
		pub:    pub,
		config: config,
	}, nil
}

type publisher struct {
	pub    message.Publisher
	config PublisherConfig
}

func (p *publisher) Publish(topic string, messages ...*message.Message) error {
	for i := range messages {
		p.applyDelay(messages[i])
	}
	return p.pub.Publish(topic, messages...)
}

func (p *publisher) Close() error {
	return p.pub.Close()
}

func (p *publisher) applyDelay(msg *message.Message) {
	if msg.Metadata.Get(DelayedForKey) != "" {
		return
	}

	if msg.Context().Value(delayContextKey) != nil {
		delay := msg.Context().Value(delayContextKey).(Delay)
		Message(msg, delay)
		return
	}

	if !p.config.DefaultDelay.IsZero() {
		Message(msg, p.config.DefaultDelay)
	}
}
