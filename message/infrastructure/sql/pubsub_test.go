package sql_test

import (
	std_sql "database/sql"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/internal"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"

	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func newPubSub(t *testing.T, adapter sql.SQLAdapter, consumerGroup string) message.PubSub {
	logger := watermill.NewStdLogger(false, false)

	publisher, err := sql.NewPublisher(sql.PublisherConfig{
		Adapter: adapter,
	})
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		sql.SubscriberConfig{
			Adapter:        adapter,
			Logger:         logger,
			ConsumerGroup:  consumerGroup,
			PollInterval:   100 * time.Millisecond,
			ResendInterval: 50 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	return message.NewPubSub(publisher, subscriber)
}

func getMySQLDefaultAdapter(t *testing.T) sql.SQLAdapter {
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

	require.NoError(t, db.Ping(), "could not ping database")

	adapter, err := sql.NewMySQLDefaultAdapter(db, sql.MySQLDefaultAdapterConf{
		Logger: watermill.NewStdLogger(true, true),
	})
	require.NoError(t, err)

	return adapter
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) infrastructure.PubSub {
	return newPubSub(t, getMySQLDefaultAdapter(t), consumerGroup).(infrastructure.PubSub)
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

	if testing.Short() && !internal.RaceEnabled {
		t.Log("Running only TestPublishSubscribe for SQL with -short flag")
		infrastructure.TestPublishSubscribe(t, createPubSub(t), features)
		return
	}

	infrastructure.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
