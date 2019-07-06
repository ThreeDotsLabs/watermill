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
	// BufferSize configures how many bytes will be read at a time from the Subscriber's Reader.
	// Each message will be treated as having at most BufferSize bytes.
	// If 0, Subscriber works in delimiter mode - it scans for messages delimited by the MessageDelimiter byte.
	BufferSize int
	// MessageDelimiter is the byte that is expected to separate messages if BufferSize is equal to 0.
	MessageDelimiter byte

	// PollInterval is the time between polling for new messages if the last read was empty. Defaults to time.Second.
	PollInterval time.Duration

	// UnmarshalFunc transforms the raw bytes into a Watermill message. Its behavior may be dependent on the topic.
	UnmarshalFunc UnmarshalMessageFunc
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
}

// Subscriber reads bytes from its underlying io.Reader and interprets them as Watermill messages.
// It posts the messages on the output stream from Subscribe().
// There are several ways in which Subscriber may interpret messages from the Reader, configurable by the
// unmarshal function in the config.
type Subscriber struct {
	rc          io.ReadCloser
	subscribeWg sync.WaitGroup
	config      SubscriberConfig

	closed  bool
	closing chan struct{}

	logger watermill.LoggerAdapter
}

func NewSubscriber(rc io.ReadCloser, config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid subscriber config")
	}
	config.setDefaults()

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Subscriber{
		rc:      rc,
		config:  config,
		closing: make(chan struct{}),
		logger:  logger,
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
	defer close(output)

	var reader *bufio.Reader
	if s.config.BufferSize > 0 {
		reader = bufio.NewReaderSize(s.rc, s.config.BufferSize)
	} else {
		reader = bufio.NewReader(s.rc)
	}

	var chunk []byte
	var alive bool
	readCh := s.read(reader)
	for {
		select {
		case chunk, alive = <-readCh:
			if !alive {
				s.logger.Debug("Read channel closed, breaking read loop", nil)
				return
			}
		case <-s.closing:
			s.logger.Debug("Subscriber closing, breaking read loop", nil)
			return
		}

		if s.config.BufferSize == 0 && chunk[len(chunk)-1] == s.config.MessageDelimiter {
			// trim the delimiter byte
			chunk = chunk[:len(chunk)-1]
		}

		msg, err := s.config.UnmarshalFunc(topic, chunk)
		if err != nil {
			s.logger.Error("Could not unmarshal message", err, nil)
			continue
		}
		logger := s.logger.With(watermill.LogFields{
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
}

func (s *Subscriber) read(reader *bufio.Reader) chan []byte {
	chunkCh := make(chan []byte)

	go func() {
		// todo: no way to stop this goroutine if it blocks on Read/ReadSlice
		defer func() {
			close(chunkCh)
		}()
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
				s.logger.Error("Could not read from buffer, closing read()", err, nil)
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
