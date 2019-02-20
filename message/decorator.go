package message

import (
	"context"
	"sync"
)

// MessageTransformSubscriberDecorator creates a subscriber decorator that calls transform
// on each message that passes through the subscriber.
func MessageTransformSubscriberDecorator(transform func(*Message)) SubscriberDecorator {
	if transform == nil {
		panic("transform function is nil")
	}
	return func(sub Subscriber) (Subscriber, error) {
		return &messageTransformSubscriberDecorator{
			sub:       sub,
			transform: transform,
		}, nil
	}
}

// MessageTransformPublisherDecorator creates a publisher decorator that calls transform
// on each message that passes through the publisher.
func MessageTransformPublisherDecorator(transform func(*Message)) PublisherDecorator {
	if transform == nil {
		panic("transform function is nil")
	}
	return func(pub Publisher) (Publisher, error) {
		return &messageTransformPublisherDecorator{
			Publisher: pub,
			transform: transform,
		}, nil
	}
}

type messageTransformSubscriberDecorator struct {
	sub Subscriber

	transform   func(*Message)
	subscribeWg sync.WaitGroup
}

func (t *messageTransformSubscriberDecorator) Subscribe(ctx context.Context, topic string) (<-chan *Message, error) {
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

func (t *messageTransformSubscriberDecorator) Close() error {
	err := t.sub.Close()

	t.subscribeWg.Wait()
	return err
}

type messageTransformPublisherDecorator struct {
	Publisher
	transform func(*Message)
}

// Publish applies the transform to each message and returns the underlying Publisher's result.
func (d messageTransformPublisherDecorator) Publish(topic string, messages ...*Message) error {
	for i := range messages {
		d.transform(messages[i])
	}
	return d.Publisher.Publish(topic, messages...)
}
