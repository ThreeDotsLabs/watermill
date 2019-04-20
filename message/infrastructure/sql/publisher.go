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
	// Inserter provides methods for inserting the published messages that are schema-dependent.
	Inserter Inserter
	// MessagesTable is the name of the table that will store the published messages. Defaults to `messages`.
	MessagesTable string
}

func (c PublisherConfig) validate() error {
	if c.Inserter == nil {
		return errors.New("inserter is nil")
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

	executor   db
	insertStmt *sql.Stmt

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

	insertQ := conf.Inserter.InsertQuery(conf.MessagesTable)
	insertStmt, err := db.Prepare(insertQ)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare the insert message statement")
	}

	return &Publisher{
		conf: conf,

		insertStmt: insertStmt,

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

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	for _, msg := range messages {
		insertArgs, err := p.conf.Inserter.InsertArgs(topic, msg)
		if err != nil {
			return errors.Wrap(err, "could not marshal message into insert args")
		}

		_, err = p.insertStmt.Exec(insertArgs...)
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
