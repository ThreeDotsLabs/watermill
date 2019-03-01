package mysql_test

import (
	"context"
	"fmt"
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

	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)

	messages, err := sub.Subscribe(ctx, "sometopic")
	require.NoError(t, err)

	for msg := range messages {
		fmt.Printf("%s:%s\n", msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}
