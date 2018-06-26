package sql_test

import (
	"testing"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	sqlConn "github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/roblaszczak/gooddd/message"
	"time"
	"github.com/satori/go.uuid"
	"github.com/roblaszczak/gooddd/message/infrastructure/sql"
	"fmt"
	"github.com/roblaszczak/gooddd/domain"
	"encoding/json"
)

var testSchema = "CREATE TABLE `%s` ( " +
	"`event_no` BIGINT NOT NULL AUTO_INCREMENT, " +
	"`event_id` VARCHAR(36) NOT NULL, " +
	"`event_name` VARCHAR(64) NOT NULL, " +
	"`event_payload` JSON NOT NULL, " +
	"`event_occurred_on` TIMESTAMP NOT NULL, " +
	"`aggregate_version` INT UNSIGNED NOT NULL, " +
	"`aggregate_id` BINARY(16) NOT NULL, " +
	"`aggregate_type` VARCHAR(128) NOT NULL, " +
	"`topic` VARCHAR(128), " +
	"PRIMARY KEY (`event_no`), " +
	"UNIQUE KEY (`event_id`), " +
	"UNIQUE KEY `ix_unique_event` (`aggregate_type`, `aggregate_id`, `aggregate_version`) " +
	");"

type testDomainEvent struct {
	message.Message

	occurredOn  time.Time
	aggregateID []byte

	ExtraValue string
}

type dbEvent struct {
	EventID         string    `db:"event_id"`
	EventName       string    `db:"event_name"`
	EventPayload    []byte    `db:"event_payload"`
	EventOccurredOn time.Time `db:"event_occurred_on"`

	AggregateVersion int    `db:"aggregate_version"`
	AggregateID      []byte `db:"aggregate_id"`
	AggregateType    string `db:"aggregate_type"`

	Topic string `db:"topic"`
}

func (t testDomainEvent) EventOccurredOn() time.Time {
	return t.occurredOn
}

func (t testDomainEvent) EventName() string {
	return "test"
}

func (t testDomainEvent) AggregateID() []byte {
	return t.aggregateID
}

func (t testDomainEvent) AggregateType() string {
	return "test_aggregate"
}

func (t testDomainEvent) AggregateVersion() int {
	return 0
}

func TestDomainEventsPublisher_Publish(t *testing.T) {
	conn, err := sqlConn.Open("mysql", "root:secret@/test?parseTime=true")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, conn.Close())
	}()

	tableName := fmt.Sprintf("events_test_%d_%s", time.Now().Unix(), uuid.NewV4().String())
	_, err = conn.Exec(fmt.Sprintf(testSchema, tableName))
	require.NoError(t, err)
	defer func() {
		_, err := conn.Exec("DROP TABLE `" + tableName + "`")
		assert.NoError(t, err)
	}()

	publieher := sql.NewDomainEventsPublisher(conn, tableName)

	var eventsToPublish []message.Message
	extraValues := map[string]string{}

	for i := 0; i < 100; i++ {
		id := uuid.NewV4().String()
		extraValue := uuid.NewV4().String()

		msg := message.NewDefault(id, nil)
		msg.SetMetadata("test", uuid.NewV4().String())

		occurredOn := time.Date(2009, 11, 17, 20, 34, 13, 0, time.UTC).Add(time.Minute * time.Duration(i))
		eventsToPublish = append(eventsToPublish, testDomainEvent{
			Message:     msg,
			occurredOn:  occurredOn,
			aggregateID: uuid.NewV4().Bytes(),
			ExtraValue:  extraValue,
		})
		extraValues[id] = extraValue
	}

	topicName := "topic_name"
	err = publieher.Publish(topicName, eventsToPublish)
	require.NoError(t, err)

	r := conn.QueryRow("SELECT COUNT(*) FROM `" + tableName + "`")
	var addedEvents int
	err = r.Scan(&addedEvents)
	require.NoError(t, err)

	require.Equal(t, len(eventsToPublish), addedEvents)

	var dbEvents []dbEvent
	err = conn.Select(
		&dbEvents,
		"SELECT "+
			"event_id, event_name, event_payload, event_occurred_on, "+
			"aggregate_version, aggregate_id, aggregate_type, topic "+
			"FROM `"+ tableName+ "`",
	)
	require.NoError(t, err)

	for key, msgToPublish := range eventsToPublish {
		eventToPublish := msgToPublish.(domain.Event)
		publishedEvent := dbEvents[key]

		assert.Equal(t, eventToPublish.UUID(), publishedEvent.EventID)
		assert.Equal(t, eventToPublish.EventName(), publishedEvent.EventName)
		assert.Equal(t, eventToPublish.EventOccurredOn().Unix(), publishedEvent.EventOccurredOn.Unix())
		assert.Equal(t, eventToPublish.AggregateVersion(), publishedEvent.AggregateVersion)
		assert.Equal(t, eventToPublish.AggregateID(), publishedEvent.AggregateID)
		assert.Equal(t, eventToPublish.AggregateType(), publishedEvent.AggregateType)

		assert.Equal(t, topicName, publishedEvent.Topic)

		payload := struct {
			Message struct {
				Metadata struct {
					Test string `json:""`
				} `json:"message_metadata"`
			} `json:"Message"`
		}{}
		json.Unmarshal(publishedEvent.EventPayload, &payload)

		assert.Equal(t, eventToPublish.GetMetadata("test"), payload.Message.Metadata.Test)
	}
}
