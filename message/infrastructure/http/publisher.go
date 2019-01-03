package http

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	// ErrPublisherClosed happens when trying to publish to a topic while the publisher is closed or closing.
	ErrPublisherClosed = errors.New("publisher is closed")
)

type MarshalMessageFunc func(topic string, msg *message.Message) (*http.Request, error)

type Publisher struct {
	client *http.Client
	logger watermill.LoggerAdapter

	marshalMessageFunc MarshalMessageFunc

	closed bool
}

func NewPublisher(marshalMessageFunc MarshalMessageFunc, logger watermill.LoggerAdapter) (*Publisher, error) {
	return NewPublisherWithClient(http.DefaultClient, marshalMessageFunc, logger)
}

func NewPublisherWithClient(client *http.Client, marshalMessageFunc MarshalMessageFunc, logger watermill.LoggerAdapter) (*Publisher, error) {
	return &Publisher{
		client:             client,
		logger:             logger,
		marshalMessageFunc: marshalMessageFunc,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	for _, msg := range messages {
		req, err := p.marshalMessageFunc(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		logFields := watermill.LogFields{
			"uuid":   msg.UUID,
			"url":    req.URL.String(),
			"method": req.Method,
		}

		resp, err := p.client.Do(req)
		if err != nil {
			return errors.Wrapf(err, "publishing message %s failed", msg.UUID)
		}

		// todo: process the response anyhow?

		err = resp.Body.Close()
		if err != nil {
			return errors.Wrapf(err, "could not close response body for message %s", msg.UUID)
		}

		p.logger.Trace("message published", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true
	return nil
}
