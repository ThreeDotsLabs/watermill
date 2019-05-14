package sql_test

import (
	std_sql "database/sql"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(false, true)
)

func newPubSub(t *testing.T, db *std_sql.DB, consumerGroup string) message.PubSub {
	schemaAdapter := &testSchema{db: db}
	publisher, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			MessagesTable: "messages_test",
			SchemaAdapter: schemaAdapter,
		})
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			Logger:        logger,
			ConsumerGroup: consumerGroup,

			MessagesTable:       "messages_test",
			MessageOffsetsTable: "offsets_acked_test",

			PollInterval:   100 * time.Millisecond,
			ResendInterval: 50 * time.Millisecond,
			SchemaAdapter:  schemaAdapter,
		},
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) infrastructure.PubSub {
	return newPubSub(t, newMySQL(t), consumerGroup).(infrastructure.PubSub)
}

func createPubSub(t *testing.T) infrastructure.PubSub {
	return createPubSubWithConsumerGroup(t, "test").(infrastructure.PubSub)
}

func TestPublishSubscribe(t *testing.T) {
	features := infrastructure.Features{
		ConsumerGroups: true,
		// todo: implement exactly once?
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	infrastructure.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
