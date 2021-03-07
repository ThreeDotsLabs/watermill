package gochannel_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func TestFanIn(t *testing.T) {
	const (
		upstreamTopicPattern = "upstream-topic-%d"
		downstreamTopic      = "downstream-topic"

		cancelAfter = time.Second

		workersCount        = 2
		messagesCount       = 99
		upstreamTopicsCount = 10
	)

	var upstreamTopics []string
	for i := 1; i <= upstreamTopicsCount; i++ {
		topic := fmt.Sprintf(upstreamTopicPattern, i)
		upstreamTopics = append(upstreamTopics, topic)
	}

	logger := watermill.NopLogger{}

	upstreamPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	fanin, err := gochannel.NewFanIn(upstreamPubSub, logger)
	require.NoError(t, err)

	fanin.AddSubscription(upstreamTopics, downstreamTopic)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	expectedNumberOfMessages := workersCount * messagesCount * upstreamTopicsCount

	receivedMessages := make(chan string, expectedNumberOfMessages)

	for i := 0; i < workersCount; i++ {
		router.AddNoPublisherHandler(
			fmt.Sprintf("worker-%v", i),
			downstreamTopic,
			fanin,
			func(msg *message.Message) error {
				receivedMessages <- string(msg.Payload)
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
		err := fanin.Run(ctx)
		require.NoError(t, err)
	}()

	<-router.Running()
	<-fanin.Running()

	for _, topic := range upstreamTopics {
		go func(topic string) {
			for i := 0; i < messagesCount; i++ {
				msg := message.NewMessage(watermill.NewUUID(), []byte(topic))
				err := upstreamPubSub.Publish(topic, msg)
				if err != nil {
					panic(err)
				}
			}
		}(topic)
	}

	<-ctx.Done()

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

func TestFanIn_AddSubscription_idempotency(t *testing.T) {
	logger := watermill.NopLogger{}
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	fanin, err := gochannel.NewFanIn(pubSub, logger)
	require.NoError(t, err)

	fanin.AddSubscription([]string{"from-topic-1", "from-topic-2"}, "to-topic-1")
	fanin.AddSubscription([]string{"from-topic-1", "from-topic-2"}, "to-topic-1")

	go func() {
		err := fanin.Run(context.Background())
		require.NoError(t, err)
	}()

	<-fanin.Running()

	err = fanin.Close()
	require.NoError(t, err)
}

func TestFanIn_RouterClosed(t *testing.T) {
	logger := watermill.NopLogger{}
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	fanin, err := gochannel.NewFanIn(pubSub, logger)
	require.NoError(t, err)

	fanin.AddSubscription([]string{"from-topic-1", "from-topic-2"}, "to-topic-1")
	fanin.AddSubscription([]string{"from-topic-1", "from-topic-2"}, "to-topic-2")

	go func() {
		err := fanin.Run(context.Background())
		require.NoError(t, err)
	}()

	<-fanin.Running()

	err = fanin.Close()
	require.NoError(t, err)
}
