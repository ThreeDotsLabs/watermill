package gochannel_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func TestFanOut(t *testing.T) {
	const (
		upstreamTopic = "upstream-topic"
	)

	logger := watermill.NopLogger{}

	upstreamPubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	fanout, err := gochannel.NewFanOut(upstreamPubSub, logger)
	require.NoError(t, err)

	fanout.AddSubscription(upstreamTopic)

	workersCount := 10
	messagesCount := 100

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	expectedNumberOfMessages := workersCount * messagesCount

	receivedMessages := make(chan struct{}, expectedNumberOfMessages)

	for i := 0; i < workersCount; i++ {
		router.AddNoPublisherHandler(
			fmt.Sprintf("worker-%v", i),
			upstreamTopic,
			fanout,
			func(msg *message.Message) error {
				receivedMessages <- struct{}{}
				return nil
			},
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		err := router.Run(ctx)
		require.NoError(t, err)
	}()

	go func() {
		err := fanout.Run(ctx)
		require.NoError(t, err)
	}()

	<-router.Running()
	<-fanout.Running()

	go func() {
		for i := 0; i < messagesCount; i++ {
			msg := message.NewMessage(watermill.NewUUID(), nil)
			err := upstreamPubSub.Publish(upstreamTopic, msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	<-ctx.Done()

	counter := 0

loop:
	for {
		select {
		case <-receivedMessages:
			counter += 1
		case <-time.After(time.Second):
			close(receivedMessages)
			break loop
		}
	}

	require.Equal(t, expectedNumberOfMessages, counter)
}

func TestFanOut_RouterClosed(t *testing.T) {
	logger := watermill.NopLogger{}
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	fanout, err := gochannel.NewFanOut(pubSub, logger)
	require.NoError(t, err)

	fanout.AddSubscription("some-topic")

	go func() {
		err := fanout.Run(context.Background())
		require.NoError(t, err)
	}()

	<-fanout.Running()

	err = fanout.Close()
	require.NoError(t, err)

	assert.True(t, fanout.IsClosed())
}
