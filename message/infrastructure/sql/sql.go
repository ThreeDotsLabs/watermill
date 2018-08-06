package sql

import (
	"database/sql"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd/components/domain"
	"encoding/json"
)

type event struct {
	ID          string
	Name        string
	JsonPayload []byte
	OccurredOn  time.Time

	AggregateVersion sql.NullInt64
	AggregateID      interface{}
	AggregateType    string

	Topic string
}

func (e event) Args() []interface{} {
	return []interface{}{
		e.ID, e.Name, e.JsonPayload, e.OccurredOn, e.AggregateVersion, e.AggregateID, e.AggregateType, e.Topic,
	}
}

type db interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type DomainEventsPublisher struct {
	db        db
	tableName string
}

// todo - add not domain event publisher type?
func NewDomainEventsPublisher(db db, table string) message.Publisher {
	return &DomainEventsPublisher{db, table}
}

func (s DomainEventsPublisher) Publish(topic string, messages []message.ProducedMessage) error {
	if len(messages) == 0 {
		return nil
	}

	domainEvents := make([]domain.Event, len(messages))
	for i, m := range messages {
		event, ok := m.(domain.Event)
		if !ok {
			return errors.Errorf("'%s' is not a valid domain.Event", m.UUID())
		}

		domainEvents[i] = event
	}

	return s.PublishDomainEvents(topic, domainEvents)
}

func (s DomainEventsPublisher) PublishDomainEvents(topic string, domainEvents []domain.Event) error {
	if len(domainEvents) == 0 {
		return nil
	}

	var args []interface{}

	for _, e := range domainEvents {
		jsonPayload, err := json.Marshal(e)
		if err != nil {
			return errors.Errorf("cannot serialize event '%s'", e.UUID())
		}

		sqlEvent := event{
			ID:            e.UUID(),
			Name:          e.Name(),
			JsonPayload:   jsonPayload,
			OccurredOn:    e.OccurredOn(),
			AggregateID:   e.AggregateID(),
			AggregateType: e.AggregateType(),
			Topic:         topic,
		}
		if ve, ok := e.(domain.VersionedEvent); ok {
			sqlEvent.AggregateVersion = sql.NullInt64{Int64: int64(ve.AggregateVersion()), Valid: true}
		}

		args = append(args, sqlEvent.Args()...)
	}

	query := "INSERT INTO " +
		"`" + s.tableName + "` (`event_id`, `event_name`, `event_payload`, `event_occurred_on`, `aggregate_version`, " +
		"`aggregate_id`, `aggregate_type`, `topic`) VALUES "
	query += strings.TrimRight(strings.Repeat("(?, ?, ?, ?, ?, ?, ?, ?),", len(domainEvents)), ",")

	_, err := s.db.Exec(query, args...)
	if err != nil {
		return errors.Wrapf(err, "cannot save events to database")
	}

	return nil
}

func (s DomainEventsPublisher) ClosePublisher() error {
	return nil
}
