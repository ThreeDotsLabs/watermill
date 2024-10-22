package delay

import "github.com/ThreeDotsLabs/watermill/message"

type DefaultDelayGeneratorParams struct {
	Topic   string
	Message *message.Message
}

// PublisherConfig is a configuration for the delay publisher.
type PublisherConfig struct {
	// DefaultDelayGenerator is a function that generates the default delay for a message.
	// If the message doesn't have the delay metadata set, the default delay will be applied.
	DefaultDelayGenerator func(params DefaultDelayGeneratorParams) (Delay, error)
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
		err := p.applyDelay(topic, messages[i])
		if err != nil {
			return err
		}
	}
	return p.pub.Publish(topic, messages...)
}

func (p *publisher) Close() error {
	return p.pub.Close()
}

func (p *publisher) applyDelay(topic string, msg *message.Message) error {
	if msg.Metadata.Get(DelayedForKey) != "" {
		return nil
	}

	if msg.Context().Value(delayContextKey) != nil {
		delay := msg.Context().Value(delayContextKey).(Delay)
		Message(msg, delay)
		return nil
	}

	if p.config.DefaultDelayGenerator != nil {
		delay, err := p.config.DefaultDelayGenerator(DefaultDelayGeneratorParams{
			Topic:   topic,
			Message: msg,
		})
		if err != nil {
			return err
		}
		Message(msg, delay)
	}

	return nil
}
