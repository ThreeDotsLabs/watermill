package sql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	"github.com/stretchr/testify/require"
)

func TestSubscriber_Subscribe(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	sub, err := sql.NewSubscriber(
		sql.SubscriberConfig{
			//Adapter:       getMySQLDefaultAdapter(t),
			Logger:        logger,
			ConsumerGroup: "cg6",
			PollInterval:  time.Second,
		},
	)
	require.NoError(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	messages, err := sub.Subscribe(ctx, "sometopic")
	require.NoError(t, err)

	for msg := range messages {
		fmt.Printf("%s:%s\n", msg.UUID, string(msg.Payload))
		msg.Ack()
	}

	require.NoError(t, sub.Close())
}
