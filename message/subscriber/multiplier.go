package subscriber

// todo - move to another package?

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Multiplier struct {
	Constructor func() message.Subscriber
	Count       int

	subs []message.Subscriber
}

// todo - add constructor

func (m *Multiplier) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	out := make(chan *message.Message)

	for i := 0; i < m.Count; i++ {
		sub := m.Constructor()
		m.subs = append(m.subs, sub)

		subOut, err := sub.Subscribe(ctx, topic)
		if err != nil {
			return nil, err
		}

		go func() {
			for msg := range subOut {
				out <- msg

			}
		}()
	}

	return out, nil
}

func (m *Multiplier) Close() error {
	for _, sub := range m.subs {
		if err := sub.Close(); err != nil {
			return err
		}
	}

	return nil
}
