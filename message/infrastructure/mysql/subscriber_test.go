package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/mysql"

	"github.com/stretchr/testify/require"
)

func TestSubscriber_Subscribe(t *testing.T) {

	sub, err := mysql.NewSubscriber(
		getDB(t),
		mysql.SubscriberConfig{
			Table:        "events",
			Offset:       0,
			Unmarshaler:  mysql.DefaultUnmarshaler{},
			Logger:       watermill.NewStdLogger(true, true),
			PollInterval: time.Second,
		},
	)
	require.NoError(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	messages, err := sub.Subscribe(ctx, "sometopic")
	require.NoError(t, err)

	for msg := range messages {
		t.Log(msg.UUID)
		msg.Ack()
	}

	t.Log("THE END")
}
