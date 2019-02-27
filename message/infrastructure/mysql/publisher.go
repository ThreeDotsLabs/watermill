package mysql

import (
	"fmt"
	"sync"

	multierror "github.com/hashicorp/go-multierror"

	_ "github.com/siddontang/go-mysql/driver"

	"database/sql"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

type PublisherConfig struct {
	Addr     string
	Database string
	Table    string

	User     string
	Password string

	Marshaler Marshaler
}

func (c PublisherConfig) validate() error {
	if c.Database == "" {
		return errors.New("database not set")
	}

	if c.Table == "" {
		return errors.New("table not set")
	}

	if c.Marshaler == nil {
		return errors.New("marshaler not set")
	}

	return nil
}

type Publisher struct {
	config PublisherConfig

	db *sql.DB

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool
}

func NewPublisher(conf PublisherConfig) (*Publisher, error) {
	if err := conf.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	dsn := fmt.Sprintf("%s:%s@%s?%s", conf.User, conf.Password, conf.Addr, conf.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "could not open mysql connection")
	}

	if err = db.Ping(); err != nil {
		return nil, errors.Wrap(err, "could not check mysql connection")
	}

	return &Publisher{
		config: conf,

		db: db,

		publishWg: &sync.WaitGroup{},
		closeCh:   make(chan struct{}),
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return ErrPublisherClosed
	}

	tx, err := p.db.Begin()
	defer func() {
		if err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				err = multierror.Append(err, rollbackErr)
			}
		} else {
			commitErr := tx.Commit()
			if commitErr != nil {
				err = multierror.Append(err, commitErr)
			}
		}

	}()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}

	stmt, err := tx.Prepare(
		fmt.Sprintf(`INSERT INTO %s (uuid, payload, topic, metadata) VALUES (?, ?, ?, ?)`, p.config.Table),
	)
	if err != nil {
		return errors.Wrap(err, "could not prepare statement")
	}

	for _, msg := range messages {
		publishErr := p.publish(stmt, topic, msg)
		if publishErr != nil {
			err = multierror.Append(err, publishErr)
		}
	}

	return err
}

func (p *Publisher) publish(stmt *sql.Stmt, topic string, msg *message.Message) error {
	p.publishWg.Add(1)
	defer p.publishWg.Done()

	args, err := p.config.Marshaler.ForInsert(topic, msg)
	if err != nil {
		return errors.Wrap(err, "could not marshal message to sql insert")
	}

	_, err = stmt.Exec(args.UUID, args.Payload, args.Topic, args.Metadata)
	if err != nil {
		return errors.Wrap(err, "could not execute statement")
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closeCh)
	p.publishWg.Wait()

	return nil
}
