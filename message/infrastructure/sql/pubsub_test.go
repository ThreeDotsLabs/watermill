package sql_test

import (
	std_sql "database/sql"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	driver "github.com/go-sql-driver/mysql"
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

func newMySQL(t *testing.T) *std_sql.DB {
	conf := driver.Config{
		User:                 "root",
		Passwd:               "",
		Net:                  "",
		Addr:                 "localhost",
		DBName:               "watermill",
		AllowNativePasswords: true,
	}
	db, err := std_sql.Open("mysql", conf.FormatDSN())
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) infrastructure.PubSub {
	return newPubSub(t, newMySQL(t), consumerGroup).(infrastructure.PubSub)
}

func createPubSub(t *testing.T) infrastructure.PubSub {
	return createPubSubWithConsumerGroup(t, "test").(infrastructure.PubSub)
}

func TestPublishSubscribe(t *testing.T) {
	features := infrastructure.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
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
