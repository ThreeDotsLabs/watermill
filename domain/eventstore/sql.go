package eventstore

import (
	"database/sql"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/domain"
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

type SQLEventSerializer func(event domain.Event) (SQLEvent, error)

type db interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type SQL struct {
	db         db
	serializer SQLEventSerializer
}

func NewSQL(db db, serializer SQLEventSerializer) domain.Eventstore {
	return &SQL{db, serializer}
}

func (s SQL) Save(events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	query := "INSERT INTO " +
		"`events` (`event_id`, `event_name`, `event_payload`, `event_occurred_on`, `aggregate_version`, " +
		"`aggregate_id`, `aggregate_type`) VALUES " + strings.Repeat("(?, ?, ?, ?, ?, ?, ?),", len(events))

	query = strings.TrimRight(query, ",")

	var args []interface{}

	for _, event := range events {
		// todo - move it somewhere(higher level)
		event, err := s.serializer(event)
		if err != nil {
			return errors.Wrap(err, "cannot serialize event")
		}

		args = append(args, event.Args()...)
	}

	_, err := s.db.Exec(query, args...)
	if err != nil {
		return errors.Wrapf(err, "cannot save events to database")
	}

	return nil
}
