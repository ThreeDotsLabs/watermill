package subscriber

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Constructor is a function that creates a subscriber.
type Constructor func() (message.Subscriber, error)

type multiplier struct {
	subscriberConstructor func() (message.Subscriber, error)
	subscribersCount      int
	subscribers           []message.Subscriber
}

// NewMultiplier returns multiplier subscriber decorator,
// which under the hood is calling subscribe multiple times to increase throughput.
func NewMultiplier(constructor Constructor, subscribersCount int) message.Subscriber {
	return &multiplier{
		subscriberConstructor: constructor,
		subscribersCount:      subscribersCount,
	}
}

func (s *multiplier) Subscribe(ctx context.Context, topic string) (msgs <-chan *message.Message, err error) {
	defer func() {
		if err != nil {
			if closeErr := s.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()

	out := make(chan *message.Message)

	subWg := sync.WaitGroup{}
	subWg.Add(s.subscribersCount)

	for i := 0; i < s.subscribersCount; i++ {
		sub, err := s.subscriberConstructor()
		if err != nil {
			return nil, fmt.Errorf("cannot create subscriber: %w", err)
		}

		s.subscribers = append(s.subscribers, sub)

		msgs, err := sub.Subscribe(ctx, topic)
		if err != nil {
			return nil, fmt.Errorf("cannot subscribe: %w", err)
		}

		go func() {
			for msg := range msgs {
				out <- msg
			}
			subWg.Done()
		}()
	}

	go func() {
		subWg.Wait()
		close(out)
	}()

	return out, nil
}

func (s *multiplier) Close() error {
	var err error

	for _, sub := range s.subscribers {
		if closeErr := sub.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}

	return err
}
