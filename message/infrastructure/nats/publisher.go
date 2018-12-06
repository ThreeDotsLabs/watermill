package nats

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/go-nats-streaming"
	"github.com/pkg/errors"
)

type PublisherConfig struct {
	ClusterID string
	ClientID  string

	StanOptions []stan.Option

	Marshaler Marshaler
}

type Publisher struct {
	conn   stan.Conn
	config PublisherConfig
	logger watermill.LoggerAdapter
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	conn, err := stan.Connect(config.ClusterID, config.ClientID, config.StanOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}

	return &Publisher{
		conn:   conn,
		config: config,
		logger: logger,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		p.logger.Trace("Publishing message", messageFields)

		b, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return err
		}

		if err := p.conn.Publish(topic, b); err != nil {
			return errors.Wrap(err, "sending message failed")
		}
	}

	return nil
}

func (p Publisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("Publisher closed", nil)

	if err := p.conn.Close(); err != nil {
		return errors.Wrap(err, "closing nats conn failed")
	}

	return nil
}
