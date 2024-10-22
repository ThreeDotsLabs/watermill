package delay_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func TestPublisher(t *testing.T) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, nil)

	messages, err := pubSub.Subscribe(context.Background(), "test")
	require.NoError(t, err)

	pub, err := delay.NewPublisher(pubSub, delay.PublisherConfig{})
	require.NoError(t, err)

	defaultDelayPub, err := delay.NewPublisher(pubSub, delay.PublisherConfig{
		DefaultDelayGenerator: func(params delay.DefaultDelayGeneratorParams) (delay.Delay, error) {
			return delay.For(1 * time.Second), nil
		},
	})
	require.NoError(t, err)

	testCases := []struct {
		name               string
		publisher          message.Publisher
		messageConstructor func(id string) *message.Message
		expectedDelay      time.Duration
	}{
		{
			name:      "no delay",
			publisher: pub,
			messageConstructor: func(id string) *message.Message {
				return message.NewMessage(id, nil)
			},
			expectedDelay: 0,
		},
		{
			name:      "default delay",
			publisher: defaultDelayPub,
			messageConstructor: func(id string) *message.Message {
				return message.NewMessage(id, nil)
			},
			expectedDelay: 1 * time.Second,
		},
		{
			name:      "delay from metadata",
			publisher: pub,
			messageConstructor: func(id string) *message.Message {
				msg := message.NewMessage(id, nil)
				delay.Message(msg, delay.For(2*time.Second))
				return msg
			},
			expectedDelay: 2 * time.Second,
		},
		{
			name:      "default delay override with metadata",
			publisher: defaultDelayPub,
			messageConstructor: func(id string) *message.Message {
				msg := message.NewMessage(id, nil)
				delay.Message(msg, delay.For(2*time.Second))
				return msg
			},
			expectedDelay: 2 * time.Second,
		},
		{
			name:      "delay from context",
			publisher: pub,
			messageConstructor: func(id string) *message.Message {
				msg := message.NewMessage(id, nil)
				ctx := delay.WithContext(context.Background(), delay.For(3*time.Second))
				msg.SetContext(ctx)
				return msg
			},
			expectedDelay: 3 * time.Second,
		},
		{
			name:      "default delay override with context",
			publisher: defaultDelayPub,
			messageConstructor: func(id string) *message.Message {
				msg := message.NewMessage(id, nil)
				ctx := delay.WithContext(context.Background(), delay.For(3*time.Second))
				msg.SetContext(ctx)
				return msg
			},
			expectedDelay: 3 * time.Second,
		},
		{
			name:      "delay with until",
			publisher: pub,
			messageConstructor: func(id string) *message.Message {
				msg := message.NewMessage(id, nil)
				delay.Message(msg, delay.Until(time.Now().UTC().Add(4*time.Second)))
				return msg
			},
			expectedDelay: 4 * time.Second,
		},
		{
			name:      "both metadata and context set",
			publisher: defaultDelayPub,
			messageConstructor: func(id string) *message.Message {
				msg := message.NewMessage(id, nil)
				delay.Message(msg, delay.For(5*time.Second))
				ctx := delay.WithContext(context.Background(), delay.For(6*time.Second))
				msg.SetContext(ctx)
				return msg
			},
			expectedDelay: 5 * time.Second,
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			id := fmt.Sprint(i)

			msg := testCase.messageConstructor(id)
			err = testCase.publisher.Publish("test", msg)
			require.NoError(t, err)

			assertMessage(t, messages, id, testCase.expectedDelay)
		})
	}
}

func assertMessage(t *testing.T, messages <-chan *message.Message, expectedID string, expectedDelay time.Duration) {
	t.Helper()
	select {
	case msg := <-messages:
		assert.Equal(t, expectedID, msg.UUID)

		if expectedDelay == 0 {
			assert.Empty(t, msg.Metadata.Get(delay.DelayedUntilKey))
			assert.Empty(t, msg.Metadata.Get(delay.DelayedForKey))
		} else {
			delayedFor, err := time.ParseDuration(msg.Metadata.Get(delay.DelayedForKey))
			require.NoError(t, err)
			assert.Equal(t, expectedDelay, delayedFor.Round(time.Second))

			delayedUntil, err := time.Parse(time.RFC3339, msg.Metadata.Get(delay.DelayedUntilKey))
			require.NoError(t, err)

			assert.WithinDuration(t, time.Now().UTC().Add(expectedDelay), delayedUntil, 1*time.Second)
		}

		msg.Ack()
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "timeout")
	}
}
