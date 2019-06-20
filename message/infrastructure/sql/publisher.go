package sql

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/pkg/errors"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

type PublisherConfig struct {
	Logger watermill.LoggerAdapter

	// SchemaAdapter provides the schema-dependent queries and arguments for them, based on topic/message etc.
	SchemaAdapter SchemaAdapter

	// AutoInitializeSchema enables initialization of schema database during publish.
	// Schema is initialized once per topic per publisher instance.
	AutoInitializeSchema bool
}

func (c PublisherConfig) validate() error {
	if c.SchemaAdapter == nil {
		return errors.New("schema adapter is nil")
	}

	return nil
}

func (c *PublisherConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

// Publisher inserts the Messages as rows into a SQL table..
type Publisher struct {
	config PublisherConfig

	db db

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool

	initializedTopics sync.Map
}

func NewPublisher(db db, config PublisherConfig) (*Publisher, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	return &Publisher{
		config: config,
		db:     db,

		publishWg: new(sync.WaitGroup),
		closeCh:   make(chan struct{}),
		closed:    false,
	}, nil
}

// Publish inserts the messages as rows into the MessagesTable.
// Order is guaranteed for messages within one call.
// Publish is blocking until all rows have been added to the Publisher's transaction.
// Publisher doesn't guarantee publishing messages in a single transaction,
// but the constructor accepts both *sql.DB and *sql.Tx, so transactions may be handled upstream by the user.
func (p *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return ErrPublisherClosed
	}

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	if err := validateTopicName(topic); err != nil {
		return err
	}

	if err := p.initializeSchema(topic); err != nil {
		return err
	}

	insertQuery, insertArgs, err := p.config.SchemaAdapter.InsertQuery(topic, messages)
	if err != nil {
		return errors.Wrap(err, "cannot create insert query")
	}

	p.config.Logger.Trace("Inserting message to SQL", watermill.LogFields{
		"query":      insertQuery,
		"query_args": sqlArgsToLog(insertArgs),
	})

	_, err = p.db.ExecContext(context.Background(), insertQuery, insertArgs...)
	if err != nil {
		return errors.Wrap(err, "could not insert message as row")
	}

	return nil
}

func (p *Publisher) initializeSchema(topic string) error {
	if !p.config.AutoInitializeSchema {
		return nil
	}

	if _, ok := p.initializedTopics.Load(topic); ok {
		return nil
	}

	if err := initializeSchema(
		context.Background(),
		topic,
		p.config.Logger,
		p.db,
		p.config.SchemaAdapter,
		nil,
	); err != nil {
		return errors.Wrap(err, "cannot initialize schema")
	}

	p.initializedTopics.Store(topic, struct{}{})
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
