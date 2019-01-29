package message

import (
	"errors"
	"sync"
)

type messageTransformer struct {
	sub Subscriber

	outputChannels     []chan *Message
	outputChannelsLock sync.Mutex

	transform func(*Message)
	onClose   func(error)

	closed  bool
	closing chan struct{}
}

func (t *messageTransformer) Subscribe(topic string) (chan *Message, error) {
	if t.closed {
		return nil, errors.New("subscriber closed")
	}

	t.outputChannelsLock.Lock()
	defer t.outputChannelsLock.Unlock()

	out := make(chan *Message)

	in, err := t.sub.Subscribe(topic)
	if err != nil {
		return nil, err
	}

	t.outputChannels = append(t.outputChannels, out)

	go func() {
		for {
			select {
			case msg := <-in:
				if msg == nil {
					continue
				}
				t.transform(msg)
				out <- msg

			case <-t.closing:
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

	defer t.onClose(err)

	close(t.closing)

	t.outputChannelsLock.Lock()
	for _, ch := range t.outputChannels {
		close(ch)
	}
	t.outputChannels = nil
	t.outputChannelsLock.Unlock()

	return t.sub.Close()
}

// MessageTransformSubscriberDecorator creates a subscriber decorator that calls transform on each that passes
// through the subscriber. When Close is called on the decorated subscriber, it closes the wrapped subscriber and calls
// onClose with the resulting error.
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

			outputChannels:     []chan *Message{},
			outputChannelsLock: sync.Mutex{},

			transform: transform,
			onClose:   onClose,

			closing: make(chan struct{}),
		}, nil
	}
}
