package sql

import (
	"database/sql"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

type SQLEvent struct {
	ID          interface{}
	Name        string
	JsonPayload []byte
	OccurredOn  time.Time

	AggregateVersion int
	AggregateID      interface{}
	AggregateType    string
}

func (e SQLEvent) Args() []interface{} {
	return []interface{}{
		e.ID, e.Name, e.JsonPayload, e.OccurredOn, e.AggregateVersion, e.AggregateID, e.AggregateType,
	}
}

type SQLEventSerializer func(message.Message) (SQLEvent, error)

type db interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type SQL struct {
	db         db
	serializer SQLEventSerializer
}

func NewSQL(db db, serializer SQLEventSerializer) message.Publisher {
	return &SQL{db, serializer}
}

func (s SQL) Publish(messages []message.Message) error {
	if len(messages) == 0 {
		return nil
	}

	query := "INSERT INTO " +
		"`events` (`event_id`, `event_name`, `event_payload`, `event_occurred_on`, `aggregate_version`, " +
		"`aggregate_id`, `aggregate_type`) VALUES " + strings.Repeat("(?, ?, ?, ?, ?, ?, ?),", len(messages))

	query = strings.TrimRight(query, ",")

	var args []interface{}

	for _, m := range messages {
		event, err := s.serializer(m)
		if err != nil {
			return errors.Wrap(err, "cannot serialize message")
		}

		args = append(args, event.Args()...)
	}

	_, err := s.db.Exec(query, args...)
	if err != nil {
		return errors.Wrapf(err, "cannot save events to database")
	}

	return nil
}

func (s SQL) Close() error {
	return nil
}
