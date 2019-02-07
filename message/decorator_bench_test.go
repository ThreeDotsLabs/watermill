package message_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
)

type benchSubscriber struct {
	msgCh   chan *message.Message
	closeCh chan struct{}
	running sync.Mutex
}

func (b *benchSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return b.msgCh, nil
}

func (b *benchSubscriber) Close() error {
	close(b.closeCh)
	b.running.Lock()
	close(b.msgCh)
	b.running.Unlock()
	return nil
}

func newBenchSubscriber() *benchSubscriber {
	sub := &benchSubscriber{
		msgCh:   make(chan *message.Message, 1),
		closeCh: make(chan struct{}),
	}

	// continually produce messages until Close()
	go func() {
		sub.running.Lock()
		msg := message.NewMessage("", []byte{})
		for {
			select {
			case <-sub.closeCh:
				sub.running.Unlock()
				return
			case sub.msgCh <- msg:
				// the buffer limit is 1, so this will block until someone consumes
			}
		}
	}()

	return sub
}

func BenchmarkMessageTransformSubscriberDecorator(b *testing.B) {
	b.ReportAllocs()
	b.Run("no_decorator", benchmarkNoDecorator)
	b.Run("message_transform_decorator", benchmarkMessageTransformSubscriberDecorator)
}

func benchmarkNoDecorator(b *testing.B) {
	sub := newBenchSubscriber()

	in, err := sub.Subscribe(context.Background(), "")
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// consume one message
		<-in
	}
}

func benchmarkMessageTransformSubscriberDecorator(b *testing.B) {
	sub := newBenchSubscriber()

	noopDecorator := message.MessageTransformSubscriberDecorator(func(*message.Message) {})
	decoratedSub, err := noopDecorator(sub)
	require.NoError(b, err)

	in, err := decoratedSub.Subscribe(context.Background(), "")
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// consume one message
		<-in
	}
}
