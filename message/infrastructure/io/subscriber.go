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
	go s.consume(ctx, topic, out)

	return out, nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)

	err := s.rc.Close()

	s.subscribeWg.Wait()
	return err
}

func (s *Subscriber) consume(ctx context.Context, topic string, output chan *message.Message) {
	defer s.subscribeWg.Done()

	var reader *bufio.Reader
	if s.config.BufferSize > 0 {
		reader = bufio.NewReaderSize(s.rc, s.config.BufferSize)
	} else {
		reader = bufio.NewReader(s.rc)
	}

	for chunk := range s.read(reader) {
		msg, err := s.config.UnmarshalFunc(topic, chunk)
		if err != nil {
			s.config.Logger.Error("Could not unmarshal message", err, watermill.LogFields{})
		}
		logger := s.config.Logger.With(watermill.LogFields{
			"uuid":  msg.UUID,
			"topic": topic,
		})

	ResendLoop:
		for {
			select {
			case output <- msg:
				logger.Trace("Message consumed", nil)
			case <-ctx.Done():
				logger.Info("Context closed, discarding message", nil)
				return
			case <-s.closing:
				logger.Info("Subscriber closed, discarding message", nil)
				return
			}

			select {
			case <-msg.Acked():
				logger.Trace("Message acked", nil)
				break ResendLoop
			case <-msg.Nacked():
				logger.Trace("Message nacked, resending", nil)
				msg = msg.Copy()
				continue ResendLoop
			case <-ctx.Done():
				logger.Info("Context closed without ack", nil)
				return
			case <-s.closing:
				logger.Info("Subscriber closed without ack", nil)
				return
			}
		}
	}

	s.config.Logger.Trace("Reader channel closed", nil)
}

func (s *Subscriber) read(reader *bufio.Reader) chan []byte {
	chunkCh := make(chan []byte)

	go func() {
		// todo: no way to stop this goroutine if it blocks on Read/ReadSlice
		for {
			var bytesRead int
			var err error

			var chunk []byte
			if s.config.BufferSize > 0 {
				chunk = make([]byte, s.config.BufferSize)
			}

			if s.config.BufferSize > 0 {
				bytesRead, err = reader.Read(chunk)
			} else {
				chunk, err = reader.ReadSlice(s.config.MessageDelimiter)
				bytesRead = len(chunk)
			}

			if err != nil && errors.Cause(err) != io.EOF {
				s.config.Logger.Error("Could not read from buffer, closing read()", err, watermill.LogFields{})
				close(chunkCh)
				return
			}

			if s.closed {
				return
			}

			if bytesRead == 0 {
				time.Sleep(s.config.PollInterval)
				continue
			}

			chunkCh <- chunk
		}
	}()

	return chunkCh
}
