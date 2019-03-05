package sql

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

type PublisherConfig struct {
	Adapter SQLAdapter
}

func (c PublisherConfig) validate() error {
	if c.Adapter == nil {
		return errors.New("adapter is nil")
	}

	return nil
}

// Publisher does ... (todo)
type Publisher struct {
	conf PublisherConfig

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool
}

func NewPublisher(conf PublisherConfig) (*Publisher, error) {
	if err := conf.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &Publisher{
		conf:      conf,
		publishWg: new(sync.WaitGroup),
		closeCh:   make(chan struct{}),
	}, nil
}

// Publish inserts the messages as rows into the SQL database.
// Publish is blocking and transactional, so:
// - the function returns only after write has completed or failed.
// - either all or no messages from one Publish call will be written.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	return p.conf.Adapter.InsertMessages(context.Background(), topic, messages...)
}

// Close closes the publisher, which means that all the Publish calls called before are completed
// and no more Publish calls are accepted.
// Close is blocking until all the ongoing Publish calls have returned.
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closeCh)
	p.publishWg.Wait()

	return nil
}
