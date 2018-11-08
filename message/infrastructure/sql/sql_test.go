package sql

// todo - fix
//import (
//	"testing"
//	_ "github.com/go-sql-driver/mysql"
//	"github.com/stretchr/testify/require"
//	sqlConn "github.com/jmoiron/sqlx"
//	"github.com/stretchr/testify/assert"
//	"github.com/ThreeDotsLabs/watermill/message"
//	"time"
//	"github.com/satori/go.uuid"
//	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"
//	"fmt"
//	"github.com/ThreeDotsLabs/watermill/components/domain"
//	"encoding/json"
//	stdSQL "database/sql"
//)
//
//var testSchema = "CREATE TABLE `%s` ( " +
//	"`event_no` BIGINT NOT NULL AUTO_INCREMENT, " +
//	"`event_id` VARCHAR(36) NOT NULL, " +
//	"`event_name` VARCHAR(64) NOT NULL, " +
//	"`event_payload` JSON NOT NULL, " +
//	"`event_occurred_on` TIMESTAMP NOT NULL, " +
//	"`aggregate_version` INT UNSIGNED, " +
//	"`aggregate_id` BINARY(16) NOT NULL, " +
//	"`aggregate_type` VARCHAR(128) NOT NULL, " +
//	"`topic` VARCHAR(128), " +
//	"`metadata` TEXT, " +
//	"PRIMARY KEY (`event_no`), " +
//	"UNIQUE KEY (`event_id`), " +
//	"UNIQUE KEY `ix_unique_event` (`aggregate_type`, `aggregate_id`, `aggregate_version`) " +
//	");"
//
//type testDomainEvent struct {
//	message.Message
//
//	occurredOn  time.Time
//	aggregateID []byte
//
//	ExtraValue string
//
//	Version int
//}
//
//type dbEvent struct {
//	EventID         string    `db:"event_id"`
//	EventName       string    `db:"event_name"`
//	EventPayload    []byte    `db:"event_payload"`
//	EventOccurredOn time.Time `db:"event_occurred_on"`
//
//	AggregateVersion stdSQL.NullInt64 `db:"aggregate_version"`
//	AggregateID      []byte           `db:"aggregate_id"`
//	AggregateType    string           `db:"aggregate_type"`
//
//	Topic string `db:"topic"`
//	Metadata string `db:"metadata"`
//}
//
//func (t testDomainEvent) OccurredOn() time.Time {
//	return t.occurredOn
//}
//
//func (t testDomainEvent) Name() string {
//	return "test"
//}
//
//func (t testDomainEvent) AggregateID() []byte {
//	return t.aggregateID
//}
//
//func (t testDomainEvent) AggregateType() string {
//	return "test_aggregate"
//}
//
//func (t testDomainEvent) AggregateVersion() int {
//	return t.Version
//}
//
//func TestDomainEventsPublisher_Publish(t *testing.T) {
//	conn, err := sqlConn.Open("mysql", "root:secret@/test?parseTime=true")
//	require.NoError(t, err)
//	defer func() {
//		assert.NoError(t, conn.Close())
//	}()
//
//	tableName := fmt.Sprintf("events_test_%d_%s", time.Now().Unix(), uuid.NewV4().String())
//	_, err = conn.Exec(fmt.Sprintf(testSchema, tableName))
//	require.NoError(t, err)
//	defer func() {
//		_, err := conn.Exec("DROP TABLE `" + tableName + "`")
//		assert.NoError(t, err)
//	}()
//
//	publieher := sql.NewDomainEventsPublisher(conn, tableName)
//
//	var eventsToPublish []message.Message
//	extraValues := map[string]string{}
//
//	for i := 0; i < 100; i++ {
//		id := uuid.NewV4().String()
//		extraValue := uuid.NewV4().String()
//
//		msg := message.NewMessage(id, nil)
//		msg.SetMetadata("test", uuid.NewV4().String())
//
//		occurredOn := time.Date(2009, 11, 17, 20, 34, 13, 0, time.UTC).Add(time.Minute * time.Duration(i))
//		eventsToPublish = append(eventsToPublish, testDomainEvent{
//			Message:     msg,
//			occurredOn:  occurredOn,
//			aggregateID: uuid.NewV4().Bytes(),
//			ExtraValue:  extraValue,
//		})
//		extraValues[id] = extraValue
//	}
//
//	sameAggregateID := uuid.NewV4().Bytes()
//	for i := 1; i < 10; i++ {
//		msgID := uuid.NewV4().String()
//		extraValue := uuid.NewV4().String()
//
//		msg := message.NewMessage(msgID, nil)
//		msg.SetMetadata("test", uuid.NewV4().String())
//
//		occurredOn := time.
//			Date(2009, 11, 17, 20, 34, 13, 0, time.UTC).
//			Add(time.Minute * time.Duration(i))
//
//		eventsToPublish = append(eventsToPublish, testDomainEvent{
//			Message:     msg,
//			occurredOn:  occurredOn,
//			aggregateID: sameAggregateID,
//			ExtraValue:  extraValue,
//			Version:     i,
//		})
//		extraValues[msgID] = extraValue
//	}
//
//	topicName := "topic_name"
//	err = publieher.Publish(topicName, eventsToPublish)
//	require.NoError(t, err)
//
//	r := conn.QueryRow("SELECT COUNT(*) FROM `" + tableName + "`")
//	var addedEvents int
//	err = r.Scan(&addedEvents)
//	require.NoError(t, err)
//
//	require.Equal(t, len(eventsToPublish), addedEvents)
//
//	var dbEvents []dbEvent
//	err = conn.Select(
//		&dbEvents,
//		"SELECT "+
//			"event_id, event_name, event_payload, event_occurred_on, "+
//			"aggregate_version, aggregate_id, aggregate_type, topic, metadata "+
//			"FROM `"+ tableName+ "`",
//	)
//	require.NoError(t, err)
//
//	for key, msgToPublish := range eventsToPublish {
//		eventToPublish := msgToPublish.(domain.Event)
//		publishedEvent := dbEvents[key]
//
//		assert.Equal(t, eventToPublish.UUID(), publishedEvent.EventID)
//		assert.Equal(t, eventToPublish.Name(), publishedEvent.EventName)
//		assert.Equal(t, eventToPublish.OccurredOn().Unix(), publishedEvent.EventOccurredOn.Unix())
//
//		expectedAggregateVersion := stdSQL.NullInt64{}
//		if versionedEvent, ok := eventToPublish.(domain.VersionedEvent); ok {
//			expectedAggregateVersion = stdSQL.NullInt64{Int64: int64(versionedEvent.AggregateVersion()), Valid: true}
//		}
//		assert.Equal(t, expectedAggregateVersion, publishedEvent.AggregateVersion)
//
//		assert.Equal(t, eventToPublish.AggregateID(), publishedEvent.AggregateID)
//		assert.Equal(t, eventToPublish.AggregateType(), publishedEvent.AggregateType)
//
//		assert.Equal(t, topicName, publishedEvent.Topic)
//
//		payload := struct {
//			Test string `json:""`
//		}{}
//		json.Unmarshal([]byte(publishedEvent.Metadata), &payload)
//
//		assert.Equal(t, eventToPublish.GetMetadata("test"), payload.Test)
//	}
//}
