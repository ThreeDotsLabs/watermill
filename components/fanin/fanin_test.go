package fanin_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/fanin"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func TestFanIn(t *testing.T) {
	const (
		upstreamTopicPattern = "upstream-topic-%d"
		downstreamTopic      = "downstream-topic"

		cancelAfter = time.Millisecond * 100

		workersCount        = 3
		messagesCount       = 10
		upstreamTopicsCount = 5
	)

	var upstreamTopics []string
	for i := 1; i <= upstreamTopicsCount; i++ {
		topic := fmt.Sprintf(upstreamTopicPattern, i)
		upstreamTopics = append(upstreamTopics, topic)
	}

	logger := watermill.NopLogger{}

	pubsub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	fi, err := fanin.NewFanIn(
		pubsub,
		pubsub,
		fanin.Config{
			SourceTopics: upstreamTopics,
			TargetTopic:  downstreamTopic,
		},
		logger,
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	expectedNumberOfMessages := workersCount * messagesCount * upstreamTopicsCount

	receivedMessages := make(chan string, expectedNumberOfMessages)

	for i := 0; i < workersCount; i++ {
		router.AddConsumerHandler(
			fmt.Sprintf("worker-%v", i),
			downstreamTopic,
			pubsub,
			func(msg *message.Message) error {
				payload := string(msg.Payload)
				receivedMessages <- payload
				return nil
			},
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cancelAfter)
	defer cancel()

	go func() {
		err := router.Run(ctx)
		require.NoError(t, err)
	}()

	go func() {
		err := fi.Run(ctx)
		require.NoError(t, err)
	}()

	<-router.Running()
	<-fi.Running()

	var wg sync.WaitGroup
	wg.Add(len(upstreamTopics) * messagesCount)
	for _, topic := range upstreamTopics {
		go func(topic string) {
			for i := 0; i < messagesCount; i++ {
				msg := message.NewMessage(watermill.NewUUID(), []byte(topic))
				err := pubsub.Publish(topic, msg)
				require.NoError(t, err)

				wg.Done()
			}
		}(topic)
	}
	wg.Wait()

	counts := map[string]int{}
loop:
	for {
		select {
		case msg := <-receivedMessages:
			counts[msg]++
		case <-time.After(cancelAfter):
			close(receivedMessages)
			break loop
		}
	}

	sum := 0
	require.Len(t, counts, upstreamTopicsCount)
	for _, count := range counts {
		require.Equal(t, workersCount*messagesCount, count)
		sum += count
	}
	require.Equal(t, expectedNumberOfMessages, sum)
}

func TestNewFanIn(t *testing.T) {
	pubsub := gochannel.NewGoChannel(gochannel.Config{}, nil)

	t.Run("error when subscriber nil", func(t *testing.T) {
		_, err := fanin.NewFanIn(
			nil,
			nil,
			fanin.Config{},
			nil,
		)
		require.EqualError(t, err, "missing subscriber")
	})

	t.Run("error when publisher nil", func(t *testing.T) {
		_, err := fanin.NewFanIn(
			pubsub,
			nil,
			fanin.Config{},
			nil,
		)
		require.EqualError(t, err, "missing publisher")
	})

	t.Run("error when sourceTopics empty", func(t *testing.T) {
		_, err := fanin.NewFanIn(
			pubsub,
			pubsub,
			fanin.Config{},
			nil,
		)
		require.EqualError(t, err, "sourceTopics must not be empty")
	})

	t.Run("error when sourceTopics empty", func(t *testing.T) {
		_, err := fanin.NewFanIn(
			pubsub,
			pubsub,
			fanin.Config{
				SourceTopics: []string{""},
			},
			nil,
		)
		require.EqualError(t, err, "sourceTopics must not be empty")
	})

	t.Run("error when targetTopic empty", func(t *testing.T) {
		_, err := fanin.NewFanIn(
			pubsub,
			pubsub,
			fanin.Config{
				SourceTopics: []string{"topic"},
				TargetTopic:  "",
			},
			nil,
		)
		require.EqualError(t, err, "targetTopic must not be empty")
	})

	t.Run("error when sourceTopics contains targetTopic", func(t *testing.T) {
		_, err := fanin.NewFanIn(
			pubsub,
			pubsub,
			fanin.Config{
				SourceTopics: []string{"topic"},
				TargetTopic:  "topic",
			},
			nil,
		)
		require.EqualError(t, err, "sourceTopics must not contain targetTopic")
	})

	t.Run("correct", func(t *testing.T) {
		_, err := fanin.NewFanIn(
			pubsub,
			pubsub,
			fanin.Config{
				SourceTopics: []string{"topic"},
				TargetTopic:  "targetTopic",
			},
			nil,
		)
		require.NoError(t, err)
	})
}

func TestFanIn_source_subscription_ended(t *testing.T) {
	subscriber := &topicsSubscriberMock{
		topics: map[string]chan *message.Message{
			"topic_1": make(chan *message.Message),
			"topic_2": make(chan *message.Message),
		},
	}
	publisher := &closablePublisherMock{}
	logger := watermill.NewCaptureLogger()

	fi, err := fanin.NewFanIn(
		subscriber,
		publisher,
		fanin.Config{
			SourceTopics: []string{"topic_1", "topic_2"},
			TargetTopic:  "target_topic",
			CloseTimeout: time.Second,
		},
		logger,
	)
	require.NoError(t, err)

	runFinished := make(chan struct{})
	go func() {
		defer close(runFinished)
		assert.NoError(t, fi.Run(context.Background()))
	}()
	<-fi.Running()

	// the subscription for one of the source topics ends, for example because the consumer was closed by the broker
	close(subscriber.topics["topic_1"])

	require.Eventually(t, func() bool {
		return logger.Has(watermill.CapturedMessage{
			Level:  watermill.InfoLogLevel,
			Fields: watermill.LogFields{"subscriber_name": "fan_in_topic_1", "topic": "topic_1"},
			Msg:    "Subscriber stopped",
		})
	}, time.Second, time.Millisecond, "handler for topic_1 did not stop")

	msg := message.NewMessage(watermill.NewUUID(), nil)
	subscriber.topics["topic_2"] <- msg

	select {
	case <-msg.Acked():
		// ok
	case <-msg.Nacked():
		t.Fatal("message from topic_2 was nacked, FanIn should still forward messages from the other topics")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ack")
	}

	// the router closes itself when all subscriptions have ended
	close(subscriber.topics["topic_2"])
	select {
	case <-runFinished:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for FanIn to stop")
	}

	assert.True(t, publisher.isClosed(), "publisher should be closed when FanIn stops")
}

// topicsSubscriberMock returns a separate channel for each topic, so the test can end subscriptions independently.
type topicsSubscriberMock struct {
	topics map[string]chan *message.Message
}

func (s *topicsSubscriberMock) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return s.topics[topic], nil
}

func (s *topicsSubscriberMock) Close() error {
	return nil
}

// closablePublisherMock fails to publish after it was closed, like real publishers do.
type closablePublisherMock struct {
	closed atomic.Bool
}

func (p *closablePublisherMock) Publish(topic string, messages ...*message.Message) error {
	if p.closed.Load() {
		return errors.New("publisher closed")
	}
	return nil
}

func (p *closablePublisherMock) Close() error {
	p.closed.Store(true)
	return nil
}

func (p *closablePublisherMock) isClosed() bool {
	return p.closed.Load()
}
