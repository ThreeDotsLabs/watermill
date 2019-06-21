package io

import (
	"io"
	"sync"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type PublisherConfig struct {
	// MarshalFunc transforms the Watermill messages into raw bytes for transport.
	// Its behavior may be dependent on the topic.
	MarshalFunc MarshalMessageFunc
}

func (p PublisherConfig) validate() error {
	if p.MarshalFunc == nil {
		return errors.New("marshal func is empty")
	}

	return nil
}

// Publisher writes the messages to the underlying io.Writer.
// Its behaviour is highly customizable through the choice of the marshal function in config.
type Publisher struct {
	wc        io.WriteCloser
	config    PublisherConfig
	publishWg sync.WaitGroup

	closed bool

	logger watermill.LoggerAdapter
}

func NewPublisher(wc io.WriteCloser, config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{wc: wc, config: config, logger: logger}, nil
}

// Publish writes the messages to the underlying io.Writer.
//
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed == true {
		return errors.New("publisher is closed")
	}

	p.publishWg.Add(len(messages))

	var err error
	for _, msg := range messages {
		if writeErr := p.write(topic, msg); writeErr != nil {
			err = multierror.Append(err, writeErr)
		}
	}

	return err
}

// Close closes the underlying Writer.
// Trying to publish messages with a closed publisher will throw an error.
// Close is idempotent.
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true
	err := p.wc.Close()
	p.publishWg.Wait()

	return err
}

func (p *Publisher) write(topic string, msg *message.Message) error {
	defer p.publishWg.Done()

	b, err := p.config.MarshalFunc(topic, msg)
	if err != nil {
		return errors.Wrapf(err, "could not marshal message %s", msg.UUID)
	}

	if _, err = p.wc.Write(b); err != nil {
		return errors.Wrap(err, "could not write message to output")
	}

	return nil
}
