package message

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
)

func TestRouter_publishers_of_consumer_handlers_are_not_remembered(t *testing.T) {
	testCases := []struct {
		Name       string
		Decorators []PublisherDecorator
	}{
		{
			Name: "without_decorators",
		},
		{
			Name:       "with_decorators",
			Decorators: []PublisherDecorator{MessageTransformPublisherDecorator(func(*Message) {})},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			publisher := &publisherMock{}

			r, err := NewRouter(RouterConfig{CloseTimeout: time.Second}, watermill.NopLogger{})
			require.NoError(t, err)
			r.AddPublisherDecorators(tc.Decorators...)

			// to keep at least one running handler to prevent router from closing
			r.AddHandler("long_running_handler", "topic", contextSubscriber{}, "out", publisher, PassthroughHandler)

			go func() {
				assert.NoError(t, r.Run(context.Background()))
			}()
			<-r.Running()
			defer func() {
				assert.NoError(t, r.Close())
			}()

			for i := 0; i < 100; i++ {
				h := r.AddConsumerHandler(fmt.Sprintf("consumer_handler_%d", i), "topic", contextSubscriber{}, func(*Message) error {
					return nil
				})
				require.NoError(t, r.RunHandlers(context.Background()))

				h.Stop()
				select {
				case <-h.Stopped():
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for handler to stop")
				}
			}

			r.handlersLock.RLock()
			publishersToClose := len(r.publishers)
			r.handlersLock.RUnlock()

			assert.Equal(t, 1, publishersToClose, "only the publisher of long_running_handler should be remembered, consumer handlers have no publisher to close")
		})
	}
}

// contextSubscriber closes the subscription when the context passed to Subscribe is canceled.
type contextSubscriber struct{}

func (contextSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *Message, error) {
	messages := make(chan *Message)

	go func() {
		<-ctx.Done()
		close(messages)
	}()

	return messages, nil
}

func (contextSubscriber) Close() error {
	return nil
}

type publisherMock struct{}

func (*publisherMock) Publish(topic string, messages ...*Message) error {
	return nil
}

func (*publisherMock) Close() error {
	return nil
}
