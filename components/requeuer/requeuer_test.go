package requeuer_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/requeuer"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func TestRequeue(t *testing.T) {
	logger := watermill.NewStdLogger(false, false)

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	requeue, err := requeuer.NewRequeuer(requeuer.Config{
		Subscriber:     pubSub,
		SubscribeTopic: "requeue",
		Publisher:      pubSub,
		GeneratePublishTopic: func(params requeuer.GeneratePublishTopicParams) (string, error) {
			return "test", nil
		},
		Delay: time.Millisecond * 200,
	}, logger)
	require.NoError(t, err)

	go func() {
		err := requeue.Run(context.Background())
		require.NoError(t, err)
	}()

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	pq, err := middleware.PoisonQueue(pubSub, "requeue")
	require.NoError(t, err)

	router.AddMiddleware(pq)

	receivedMessages := make(chan int, 10)

	counter := 0

	router.AddNoPublisherHandler(
		"test",
		"test",
		pubSub,
		func(msg *message.Message) error {
			i, err := strconv.Atoi(string(msg.Payload))
			if err != nil {
				return err
			}

			counter++

			if counter < 10 && i%2 == 0 {
				return errors.New("error")
			}

			receivedMessages <- i

			return nil
		},
	)

	go func() {
		err := router.Run(context.Background())
		require.NoError(t, err)
	}()

	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		msg := message.NewMessage(watermill.NewUUID(), fmt.Append(nil, i))
		err := pubSub.Publish("test", msg)
		require.NoError(t, err)
	}

	var received []int

	timeout := false
	for !timeout {
		select {
		case i := <-receivedMessages:
			received = append(received, i)
		case <-time.After(5 * time.Second):
			timeout = true
			break
		}
	}

	require.ElementsMatch(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, received)
}
