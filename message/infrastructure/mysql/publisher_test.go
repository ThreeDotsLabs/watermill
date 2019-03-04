package mysql_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/mysql"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func TestPublisher_Publish(t *testing.T) {
	pub, err := mysql.NewPublisher(
		getDB(t),
		mysql.PublisherConfig{
			Table:     "messages",
			Marshaler: mysql.DefaultMarshaler{},
		},
	)
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
