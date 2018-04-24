package eventstore

import (
	"github.com/roblaszczak/gooddd/domain"
	"database/sql"
	"strings"
	"time"
	"github.com/pkg/errors"
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

type SQLEventSerializer func(event domain.Event) SQLEvent

type SQL struct {
	db         *sql.DB
	serializer SQLEventSerializer
}

func NewSQL(db *sql.DB, serializer SQLEventSerializer) domain.Eventstore {
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
		args = append(args, s.serializer(event).Args()...)
	}

	_, err := s.db.Exec(query, args...)
	if err != nil {
		return errors.Wrapf(err, "cannot save events to database")
	}

	return nil
}
