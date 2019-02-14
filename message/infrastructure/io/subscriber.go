package io

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type SubscriberConfig struct {
	BufferSize       int
	MessageDelimiter byte

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

	for {
		var chunk []byte
		var err error
		if s.config.BufferSize > 0 {
			_, err = reader.Read(chunk)
			if err != nil {
				// log error
			}
		} else {
			chunk, err = reader.ReadSlice(s.config.MessageDelimiter)
		}

		msg, err := s.config.UnmarshalFunc(topic, chunk)
		if err != nil {
			// handle err
		}

	ResendLoop:
		for {
			select {
			case output <- msg:
			// message consumed
			case <-ctx.Done():
				return
			case <-s.closing:
				return
			}

			select {
			case <-msg.Acked():
				break ResendLoop
			case <-msg.Nacked():
				msg = msg.Copy()
				continue ResendLoop
			case <-ctx.Done():
				return
			case <-s.closing:
				return
			}
		}
	}
}
