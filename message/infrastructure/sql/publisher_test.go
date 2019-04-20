package sql_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func TestPublisher_Publish(t *testing.T) {
	pub, err := sql.NewPublisher(
		newMySQL(t),
		sql.PublisherConfig{
			Inserter: &sql.DefaultInserter{
				Logger: watermill.NewStdLogger(true, true),
			},
		})
	require.NoError(t, err)

	msg := message.NewMessage(
		watermill.NewULID(),
		[]byte(`{"foo": "bar"}`),
	)
	msg.Metadata.Set("k", "v")

	err = pub.Publish("sometopic", msg)
	require.NoError(t, err)

	malformedMsg := message.NewMessage(
		watermill.NewULID(),
		[]byte(`"foo": "bar"}`),
	)
	malformedMsg.Metadata.Set("k", "v")

	err = pub.Publish("sometopic", malformedMsg)
	require.Error(t, err)
}
