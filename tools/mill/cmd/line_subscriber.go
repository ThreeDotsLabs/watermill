package cmd

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// lineSubscriber replaces watermill-io's Subscriber, which stops reading at the first line longer
// than its 4096-byte buffer and keeps polling at EOF instead of finishing.
// Subscribe must be called at most once.
type lineSubscriber struct {
	reader *bufio.Reader

	closing   chan struct{}
	closeOnce sync.Once
	forwardWg sync.WaitGroup

	errLock sync.Mutex
	err     error
}

func newLineSubscriber(r io.Reader) *lineSubscriber {
	return &lineSubscriber{
		reader:  bufio.NewReader(r),
		closing: make(chan struct{}),
	}
}

func (s *lineSubscriber) Subscribe(ctx context.Context, _ string) (<-chan *message.Message, error) {
	lines := make(chan []byte)
	output := make(chan *message.Message)

	go s.readLines(lines)

	s.forwardWg.Add(1)
	go s.forwardLines(ctx, lines, output)

	return output, nil
}

// Close does not wait for the reading goroutine: a read from stdin cannot be interrupted, and
// waiting for it would hold up Ctrl-C until more input arrives. The goroutine ends when the read returns.
func (s *lineSubscriber) Close() error {
	s.closeOnce.Do(func() {
		close(s.closing)
	})
	s.forwardWg.Wait()
	return nil
}

func (s *lineSubscriber) Err() error {
	s.errLock.Lock()
	defer s.errLock.Unlock()
	return s.err
}

func (s *lineSubscriber) readLines(lines chan<- []byte) {
	defer close(lines)

	for {
		line, err := s.reader.ReadBytes('\n')
		if len(line) > 0 {
			select {
			case lines <- bytes.TrimSuffix(line, []byte{'\n'}):
			case <-s.closing:
				return
			}
		}

		if err == io.EOF {
			return
		}
		if err != nil {
			s.errLock.Lock()
			s.err = errors.Wrap(err, "could not read stdin")
			s.errLock.Unlock()
			return
		}
	}
}

func (s *lineSubscriber) forwardLines(ctx context.Context, lines <-chan []byte, output chan<- *message.Message) {
	defer s.forwardWg.Done()
	defer close(output)

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return
			}
			if !s.sendUntilAcked(ctx, message.NewMessage(watermill.NewUUID(), line), output) {
				return
			}
		case <-s.closing:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Resending on nack and waiting for the ack before the next line keeps the ack/nack handling
// of watermill-io's Subscriber, which lineSubscriber replaces.
func (s *lineSubscriber) sendUntilAcked(ctx context.Context, msg *message.Message, output chan<- *message.Message) bool {
	for {
		select {
		case output <- msg:
		case <-s.closing:
			return false
		case <-ctx.Done():
			return false
		}

		select {
		case <-msg.Acked():
			return true
		case <-msg.Nacked():
			msg = msg.Copy()
		case <-s.closing:
			return false
		case <-ctx.Done():
			return false
		}
	}
}
