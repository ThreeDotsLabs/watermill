package message

import "github.com/pkg/errors"

type PublisherBackend interface {
	Publish(messages []*Message) error // todo - remove topic from args?
	Close() error
}

type FactoryFunc func(payload Payload) (*Message, error)

type Publisher struct {
	backend PublisherBackend

	msgFactoryFunc FactoryFunc
}

func NewPublisher(backend PublisherBackend, msgFactoryFunc FactoryFunc) *Publisher {
	return &Publisher{backend, msgFactoryFunc}
}

func (p *Publisher) Publish(payloads []Payload) error {
	var msgs []*Message
	for _, payload := range payloads {
		msg, err := p.msgFactoryFunc(payload)
		if err != nil {
			return errors.Wrap(err, "cannot create message")
		}

		msgs = append(msgs, msg)
	}

	return p.backend.Publish(msgs)
}

func (p *Publisher) Close() error {
	return p.backend.Close()
}
