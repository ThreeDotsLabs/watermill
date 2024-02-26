package middleware_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/stretchr/testify/assert"
)

func TestDeduplicatorMiddleware(t *testing.T) {
	t.Parallel()

	count := 0
	h := middleware.NewDeduplicator(
		middleware.NewMessageHasherAdler32(1024),
		// middleware.NewMessageHasherSHA256(1024),
		time.Second,
	).Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		count++
		return nil, nil
	})

	for i := 0; i < 6; i++ { // only one should go through
		msg := message.NewMessage(
			fmt.Sprintf("first%d", i),
			[]byte("1"),
		)
		_, err := h(msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ { // only one should go through
		msg := message.NewMessage(
			fmt.Sprintf("second%d", i),
			[]byte("2"),
		)
		_, err := h(msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	assert.Equal(t, 2, count)
}

func TestDeduplicatorPublisherDecorator(t *testing.T) {
	t.Parallel()

	pubSub := gochannel.NewGoChannel(gochannel.Config{
		OutputChannelBuffer: 100,
		Persistent:          true,
	}, nil)
	defer pubSub.Close()

	const testDedupeTopic = "testTopic"
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()

	decorated, err := middleware.NewDeduplicator(
		// middleware.NewMessageHasherAdler32(1024),
		middleware.NewMessageHasherSHA256(1024),
		time.Second,
	).PublisherDecorator()(pubSub)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 6; i++ { // only one should go through
		msg := message.NewMessage(
			fmt.Sprintf("first%d", i),
			[]byte("1"),
		)
		err := decorated.Publish(testDedupeTopic, msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ { // only one should go through
		msg := message.NewMessage(
			fmt.Sprintf("second%d", i),
			[]byte("2"),
		)
		err := decorated.Publish(testDedupeTopic, msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := pubSub.Subscribe(ctx, testDedupeTopic)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for m := range got {
		count++
		m.Ack()
		t.Log("got message:", m.UUID)
	}
	assert.Equal(t, 2, count)
}
