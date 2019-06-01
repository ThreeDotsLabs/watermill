package sql_test

import (
	std_sql "database/sql"
	"os"
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
	schemaAdapter := &testSchema{
		sql.DefaultSchema{
			GenerateMessagesTableName: func(topic string) string {
				return "test_" + topic
			},
			GenerateMessagesOffsetsTableName: func(topic string) string {
				return "test_offsets_" + topic
			},
		},
	}
	publisher, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter: schemaAdapter,
		})
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			Logger:        logger,
			ConsumerGroup: consumerGroup,

			PollInterval:   100 * time.Millisecond,
			ResendInterval: 50 * time.Millisecond,
			SchemaAdapter:  schemaAdapter,
		},
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func newMySQL(t *testing.T) *std_sql.DB {
	addr := os.Getenv("WATERMILL_TEST_MYSQL_HOST")
	t.Log("Connecting to sql at " + addr)
	if addr == "" {
		addr = "localhost"
	}
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = addr

	conf.DBName = "watermill"

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
