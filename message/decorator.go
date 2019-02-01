package message

import (
	"errors"
	"sync"
	"time"
)

type messageTransformer struct {
	sub Subscriber

	outputChannelsWg sync.WaitGroup

	transform func(*Message)
	onClose   func(error)

	closed  bool
	closing chan struct{}
}

func (t *messageTransformer) Subscribe(topic string) (chan *Message, error) {
	if t.closed {
		return nil, errors.New("subscriber closed")
	}

	out := make(chan *Message)

	in, err := t.sub.Subscribe(topic)
	if err != nil {
		return nil, err
	}

	t.outputChannelsWg.Add(1)
	go func() {
		for {
			select {
			case msg := <-in:
				t.transform(msg)
				out <- msg

			case <-t.closing:
				close(out)
				time.Sleep(3 * time.Second)
				t.outputChannelsWg.Done()
				return
			}
		}
	}()

	return out, nil
}

func (t messageTransformer) Close() (err error) {
	if t.closed {
		return nil
	}

	defer func() {
		t.onClose(err)
	}()

	close(t.closing)
	t.outputChannelsWg.Wait()

	return t.sub.Close()
}

// MessageTransformSubscriberDecorator creates a subscriber decorator that calls transform
// on each message that passes through the subscriber.
// When Close is called on the decorated subscriber, it closes the wrapped subscriber
// and calls onClose with the resulting error.
func MessageTransformSubscriberDecorator(transform func(*Message), onClose func(error)) SubscriberDecorator {
	return func(sub Subscriber) (Subscriber, error) {
		if transform == nil {
			transform = func(*Message) {}
		}
		if onClose == nil {
			onClose = func(error) {}
		}
		return &messageTransformer{
			sub: sub,

			outputChannelsWg: sync.WaitGroup{},

			transform: transform,
			onClose:   onClose,

			closed:  false,
			closing: make(chan struct{}),
		}, nil
	}
}
