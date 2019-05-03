package sql_test

import (
	"strings"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func TestPublisher_Publish(t *testing.T) {
	db := newMySQL(t)
	schemaAdapter := &testSchema{db: db}
	pub, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			MessagesTable: "messages_test",
			Inserter:      schemaAdapter,
		})
	require.NoError(t, err)

	msg := message.NewMessage(
		watermill.NewShortUUID(),
		[]byte(`{"foo": "bar"}`),
	)
	msg.Metadata.Set("k", "v")

	err = pub.Publish("sometopic", msg)
	require.NoError(t, err)

	malformedMsg := message.NewMessage(
		watermill.NewShortUUID(),
		[]byte(strings.Repeat("1", 300)),
	)
	malformedMsg.Metadata.Set("k", "v")

	err = pub.Publish("sometopic", malformedMsg)
	require.Error(t, err)
}
