package sql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	"github.com/stretchr/testify/require"
)

func TestSubscriber_Subscribe(t *testing.T) {
	schemaAdapter := &sql.DefaultSchema{Logger: logger}

	sub, err := sql.NewSubscriber(
		newMySQL(t),
		sql.SubscriberConfig{
			Logger:        logger,
			ConsumerGroup: "cg6",
			PollInterval:  time.Second,
			Acker:         schemaAdapter,
			Selecter:      schemaAdapter,
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
