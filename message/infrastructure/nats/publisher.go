package nats

import (
	stan "github.com/nats-io/stan.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type StreamingPublisherConfig struct {
	// ClusterID is the NATS Streaming cluster ID.
	ClusterID string

	// ClientID is the NATS Streaming client ID to connect with.
	// ClientID can contain only alphanumeric and `-` or `_` characters.
	ClientID string

	// StanOptions are custom options for a connection.
	StanOptions []stan.Option

	// Marshaler is marshaler used to marshal messages to stan format.
	Marshaler Marshaler
}

type StreamingPublisherPublishConfig struct {
	// Marshaler is marshaler used to marshal messages to stan format.
	Marshaler Marshaler
}

func (c StreamingPublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return errors.New("StreamingPublisherConfig.Marshaler is missing")
	}

	return nil
}

func (c StreamingPublisherConfig) GetStreamingPublisherPublishConfig() StreamingPublisherPublishConfig {
	return StreamingPublisherPublishConfig{
		Marshaler: c.Marshaler,
	}
}

type StreamingPublisher struct {
	conn   stan.Conn
	config StreamingPublisherPublishConfig
	logger watermill.LoggerAdapter
}

// NewStreamingPublisher creates a new StreamingPublisher.
//
// When using custom NATS hostname, you should pass it by options StreamingPublisherConfig.StanOptions:
//		// ...
//		StanOptions: []stan.Option{
//			stan.NatsURL("nats://your-nats-hostname:4222"),
//		}
//		// ...
func NewStreamingPublisher(config StreamingPublisherConfig, logger watermill.LoggerAdapter) (*StreamingPublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	conn, err := stan.Connect(config.ClusterID, config.ClientID, config.StanOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}

	return NewStreamingPublisherWithStanConn(conn, config.GetStreamingPublisherPublishConfig(), logger)
}

func NewStreamingPublisherWithStanConn(conn stan.Conn, config StreamingPublisherPublishConfig, logger watermill.LoggerAdapter) (*StreamingPublisher, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &StreamingPublisher{
		conn:   conn,
		config: config,
		logger: logger,
	}, nil
}

// Publish publishes message to NATS.
//
// Publish will not return until an ack has been received from NATS Streaming.
// When one of messages delivery fails - function is interrupted.
func (p StreamingPublisher) Publish(topic string, messages ...*message.Message) error {
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

func (p StreamingPublisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("StreamingPublisher closed", nil)

	if err := p.conn.Close(); err != nil {
		return errors.Wrap(err, "closing NATS conn failed")
	}

	return nil
}
