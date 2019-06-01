package sql

import (
	"database/sql"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

type PublisherConfig struct {
	// SchemaAdapter provides the schema-dependent queries and arguments for them, based on topic/message etc.
	SchemaAdapter SchemaAdapter

	// MessagesTable is the name of the table that will store the published messages. Defaults to `messages`.
	MessagesTable string
}

func (c PublisherConfig) validate() error {
	if c.SchemaAdapter == nil {
		return errors.New("schema adapter is nil")
	}

	return nil
}

func (c *PublisherConfig) setDefaults() {
	if c.MessagesTable == "" {
		c.MessagesTable = "messages"
	}
}

// db is implemented both by *sql.DB and *sql.Tx
type db interface {
	Prepare(q string) (*sql.Stmt, error)
}

// Publisher inserts the Messages as rows into a SQL table..
type Publisher struct {
	conf PublisherConfig

	db db

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool
}

func NewPublisher(db db, conf PublisherConfig) (*Publisher, error) {
	conf.setDefaults()
	if err := conf.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	return &Publisher{
		conf: conf,
		db:   db,

		publishWg: new(sync.WaitGroup),
		closeCh:   make(chan struct{}),
		closed:    false,
	}, nil
}

// Publish inserts the messages as rows into the MessagesTable.
// Order is guaranteed for messages within one call.
// Publish is blocking until all rows have been added to the Publisher's transaction.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	if err := sanitizeTopicName(topic); err != nil {
		return err
	}

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	insertQ := p.conf.SchemaAdapter.InsertQuery(topic)
	stmt, err := p.db.Prepare(insertQ)
	if err != nil {
		return errors.Wrap(err, "could not prepare stmt for inserting messages")
	}

	for _, msg := range messages {
		insertArgs, err := p.conf.SchemaAdapter.InsertArgs(topic, msg)
		if err != nil {
			return errors.Wrap(err, "could not marshal message into insert args")
		}

		_, err = stmt.Exec(insertArgs...)
		if err != nil {
			return errors.Wrap(err, "could not insert message as row")
		}
	}

	return nil
}

// Close closes the publisher, which means that all the Publish calls called before are finished
// and no more Publish calls are accepted.
// Close is blocking until all the ongoing Publish calls have returned.
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closeCh)
	p.publishWg.Wait()

	return nil
}
