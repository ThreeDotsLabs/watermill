package sql_test

import (
	"context"
	std_sql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	"github.com/stretchr/testify/require"
)

func TestSubscriber_Subscribe(t *testing.T) {
	db := newMySQL(t)
	schemaAdapter := &testSchema{db: db}
	sub, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			Logger:        logger,
			ConsumerGroup: "cg6",

			MessagesTable:       "messages_test",
			MessageOffsetsTable: "offsets_acked_test",

			PollInterval: time.Second,
			Acker:        schemaAdapter,
			Selecter:     schemaAdapter,
		},
	)
	require.NoError(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	messages, err := sub.Subscribe(ctx, "sometopic")
	require.NoError(t, err)
	go publishMessages(t, db)

	for msg := range messages {
		fmt.Printf("%s:%s\n", msg.UUID, string(msg.Payload))
		msg.Ack()
	}

	require.NoError(t, sub.Close())
}

func publishMessages(t *testing.T, db *std_sql.DB) {
	pub, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			Inserter:      &testSchema{db: db},
			MessagesTable: "messages_test",
		})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		err := pub.Publish("sometopic", message.NewMessage(
			watermill.NewShortUUID(),
			[]byte(fmt.Sprintf("message_%d", i)),
		))
		if err != nil {
			t.Error(err)
		}
	}
}
