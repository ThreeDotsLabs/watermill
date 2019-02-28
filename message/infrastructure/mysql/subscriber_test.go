package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/stretchr/testify/require"
)

func TestSubscriber_Subscribe(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		ConnectionConfig: ConnectionConfig{
			Addr:     "localhost:3306",
			Database: "watermill",
			Table:    "events",
			User:     "root",
			Password: "",
		},
		Offset:       0,
		Unmarshaler:  DefaultUnmarshaler{},
		Logger:       watermill.NewStdLogger(true, true),
		PollInterval: time.Second,
	})
	require.NoError(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	messages, err := sub.Subscribe(ctx, "sometopic")
	require.NoError(t, err)

	for msg := range messages {
		t.Log(msg.UUID)
		msg.Ack()
	}

	t.Log("THE END")
}
