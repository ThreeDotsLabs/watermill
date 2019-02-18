package io

import (
	"bufio"
	"context"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type SubscriberConfig struct {
	BufferSize       int
	MessageDelimiter byte

	// PollInterval is the time between polling for new messages if the last read was empty. Defaults to time.Second.
	PollInterval time.Duration

	UnmarshalFunc UnmarshalMessageFunc

	Logger watermill.LoggerAdapter
}

func (c SubscriberConfig) validate() error {
	if c.BufferSize != 0 && c.MessageDelimiter != 0 {
		return errors.New("choose either BufferSize or MessageDelimiter")
	}

	if c.BufferSize < 0 {
		return errors.New("buffer size must be non-negative")
	}

	if c.UnmarshalFunc == nil {
		return errors.New("unmarshal func is empty")
	}

	return nil
}

func (c *SubscriberConfig) setDefaults() {
	if c.BufferSize == 0 && c.MessageDelimiter == 0 {
		c.MessageDelimiter = '\n'
	}

	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}

	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

type Subscriber struct {
	rc          io.ReadCloser
	subscribeWg sync.WaitGroup
	config      SubscriberConfig

	closed  bool
	closing chan struct{}
}

func NewSubscriber(rc io.ReadCloser, config SubscriberConfig) (*Subscriber, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid subscriber config")
	}
	config.setDefaults()

	return &Subscriber{
		rc:      rc,
		config:  config,
		closing: make(chan struct{}),
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber is closed")
	}

	out := make(chan *message.Message)
	s.subscribeWg.Add(1)
	go s.read(ctx, topic, out)

	return out, nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)

	s.subscribeWg.Wait()

	return s.rc.Close()
}

func (s Subscriber) read(ctx context.Context, topic string, output chan *message.Message) {
	defer s.subscribeWg.Done()

	var reader *bufio.Reader
	if s.config.BufferSize > 0 {
		reader = bufio.NewReaderSize(s.rc, s.config.BufferSize)
	} else {
		reader = bufio.NewReader(s.rc)
	}

	var chunk []byte
	if s.config.BufferSize > 0 {
		chunk = make([]byte, s.config.BufferSize)
	}

	for {
		var bytesRead int
		var err error
		if s.config.BufferSize > 0 {
			bytesRead, err = reader.Read(chunk)
		} else {
			chunk, err = reader.ReadSlice(s.config.MessageDelimiter)
			bytesRead = len(chunk)
		}

		if err != nil && errors.Cause(err) != io.EOF {
			s.config.Logger.Error("could not read from buffer", err, watermill.LogFields{})
		}

		if bytesRead == 0 {
			time.Sleep(s.config.PollInterval)
			continue
		}

		msg, err := s.config.UnmarshalFunc(topic, chunk)
		if err != nil {
			s.config.Logger.Error("could not unmarshal message", err, watermill.LogFields{})
		}

		logFields := watermill.LogFields{
			"uuid":  msg.UUID,
			"topic": topic,
		}

	ResendLoop:
		for {
			select {
			case output <- msg:
				s.config.Logger.Trace("message consumed", logFields)
			case <-ctx.Done():
				s.config.Logger.Info("context closed, discarding message", logFields)
				return
			case <-s.closing:
				s.config.Logger.Info("subscriber closed, discarding message", logFields)
				return
			}

			select {
			case <-msg.Acked():
				s.config.Logger.Trace("message acked", logFields)
				break ResendLoop
			case <-msg.Nacked():
				s.config.Logger.Trace("message nacked, resending", logFields)
				msg = msg.Copy()
				continue ResendLoop
			case <-ctx.Done():
				s.config.Logger.Info("context closed without ack", logFields)
				return
			case <-s.closing:
				s.config.Logger.Info("subscriber closed without ack", logFields)
				return
			}
		}
	}
}
