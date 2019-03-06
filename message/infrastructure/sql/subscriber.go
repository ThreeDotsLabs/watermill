package sql

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

type SubscriberConfig struct {
	Adapter       SQLAdapter
	Logger        watermill.LoggerAdapter
	ConsumerGroup string

	// PollInterval is the interval between subsequent SELECT queries. Must be non-negative. Defaults to 5s.
	PollInterval time.Duration

	// ResendInterval is the time to wait before resending a nacked message. Must be non-negative. Defaults to 1s.
	ResendInterval time.Duration
}

func (c *SubscriberConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
	if c.PollInterval == 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.ResendInterval == 0 {
		c.ResendInterval = time.Second
	}
}

func (c SubscriberConfig) validate() error {
	if c.Adapter == nil {
		return errors.New("adapter is nil")
	}

	// TODO: any restraint to prevent really quick polling? I think not, caveat programmator
	if c.PollInterval <= 0 {
		return errors.New("poll interval must be a positive duration")
	}

	if c.ResendInterval <= 0 {
		return errors.New("resend interval must be a positive duration")
	}

	return nil
}

// Subscriber makes SELECT queries on the chosen table with the interval defined in the config.
// The rows are unmarshaled into Watermill messages.
type Subscriber struct {
	config SubscriberConfig

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool
}

func NewSubscriber(conf SubscriberConfig) (*Subscriber, error) {
	conf.setDefaults()
	err := conf.validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	sub := &Subscriber{
		config: conf,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),
	}

	return sub, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	// propagate the information about closing subscriber through ctx
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, topic, out)
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *Subscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()

	logger := s.config.Logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
	})

	for {
		select {
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return

		case <-ctx.Done():
			logger.Info("Stopping consume, context canceled", nil)
			return

		default:
			// go on querying
		}

		msg, err := s.config.Adapter.PopMessage(ctx, topic, s.config.ConsumerGroup)
		if err != nil && errors.Cause(err) == sql.ErrNoRows {
			// wait until polling for the next message
			time.Sleep(s.config.PollInterval)
			continue
		}
		if err != nil {
			logger.Error("Could not scan rows from query", err, nil)
			continue
		}

		s.sendMessage(ctx, msg, out, logger)
	}
}

// sendMessages sends messages on the output channel.
// whenever a message is successfully sent and acked, the message's index is sent of the offsetCh.
func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) {

	originalMsg := msg

ResendLoop:
	for {
		logger = logger.With(watermill.LogFields{
			"msg_uuid": msg.UUID,
		})

		select {
		case out <- msg:
		// message sent, go on

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return
		}

		select {
		case <-msg.Acked():
			logger.Debug("Message acked", nil)
			err := s.config.Adapter.MarkAcked(ctx, originalMsg, s.config.ConsumerGroup)
			if err != nil {
				logger.Error("could not mark message as acked", err, watermill.LogFields{
					"consumer_group": s.config.ConsumerGroup,
				})
			}
			return

		case <-msg.Nacked():
			//message nacked, try resending
			logger.Debug("Message nacked, resending", nil)
			msg = msg.Copy()

			if s.config.ResendInterval != 0 {
				time.Sleep(s.config.ResendInterval)
			}

			continue ResendLoop

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return
		}
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}
