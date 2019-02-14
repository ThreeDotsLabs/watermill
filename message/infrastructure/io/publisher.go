package io

import (
	"io"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type PublisherConfig struct {
	UnmarshalFunc UnmarshalMessageFunc
}

func (p PublisherConfig) validate() error {
	if p.UnmarshalFunc == nil {
		return errors.New("UnmarshalFunc is empty")
	}

	return nil
}

// Publisher writes the messages to the underlying io.Writer.
// Its behaviour is highly customizable through the choice of the unmarshaler function in config.
type Publisher struct {
	wc     io.WriteCloser
	config PublisherConfig

	closed bool
}

func NewPublisher(wc io.WriteCloser, config PublisherConfig) (*Publisher, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	return &Publisher{wc: wc, config: config}, nil
}

// Publish writes the messages to the underlying io.Writer.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed == true {
		return errors.New("publisher is closed")
	}

	for _, msg := range messages {
		b, err := p.config.UnmarshalFunc(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "could not unmarshal message %s", msg.UUID)
		}

		if _, err = p.wc.Write(b); err != nil {
			return errors.Wrap(err, "could not write message to output")
		}
	}

	return nil
}

// Close closes the underlying Writer.
// Trying to publish messages with a closed publisher will throw an error.
// Close is idempotent.
func (p *Publisher) Close() error {
	p.closed = true
	return p.wc.Close()
}
