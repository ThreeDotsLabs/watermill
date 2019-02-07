package message

import (
	"context"
	"sync"
)

type messageTransformer struct {
	sub Subscriber

	transform   func(*Message)
	subscribeWg sync.WaitGroup
}

func (t *messageTransformer) Subscribe(ctx context.Context, topic string) (<-chan *Message, error) {
	in, err := t.sub.Subscribe(ctx, topic)
	if err != nil {
		return nil, err
	}

	out := make(chan *Message)
	t.subscribeWg.Add(1)
	go func() {
		for msg := range in {
			t.transform(msg)
			out <- msg
		}
		close(out)
		t.subscribeWg.Done()
	}()

	return out, nil
}

func (t *messageTransformer) Close() error {
	err := t.sub.Close()

	t.subscribeWg.Wait()
	return err
}

// MessageTransformSubscriberDecorator creates a subscriber decorator that calls transform
// on each message that passes through the subscriber.
// When Close is called on the decorated subscriber, it closes the wrapped subscriber
// and calls onClose with the resulting error.
func MessageTransformSubscriberDecorator(transform func(*Message)) SubscriberDecorator {
	if transform == nil {
		panic("transform function is nil")
	}
	return func(sub Subscriber) (Subscriber, error) {
		return &messageTransformer{
			sub:       sub,
			transform: transform,
		}, nil
	}
}
