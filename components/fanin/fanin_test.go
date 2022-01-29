package fanin_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
		router.AddNoPublisherHandler(
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
