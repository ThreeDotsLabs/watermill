package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

type SubscriberConfig struct {
	// MessagesTable is the name of the table to read messages from. Defaults to `messages`.
	MessagesTable string
	// OffsetsAckedTable stores the information about which consumer group has acked which message. Defaults to `offsets_acked`.
	OffsetsAckedTable string

	// ConsumerGroup marks a group of consumers that will receive each message exactly once.
	// For now, the Subscriber implementation is experimental, so using multiple consumers with the same consumer group
	// at once may be risky.
	ConsumerGroup string

	Unmarshaler Unmarshaler
	Logger      watermill.LoggerAdapter

	// PollInterval is the interval between subsequent SELECT queries. Defaults to 5s.
	PollInterval time.Duration
}

func (c *SubscriberConfig) setDefaults() {
	if c.MessagesTable == "" {
		c.MessagesTable = "messages"
	}
	if c.OffsetsAckedTable == "" {
		c.OffsetsAckedTable = "offsets_acked"
	}
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
	if c.PollInterval == 0 {
		c.PollInterval = 5 * time.Second
	}
}

func (c SubscriberConfig) validate() error {
	if c.Unmarshaler == nil {
		return errors.New("unmarshaler not set")
	}

	// TODO: any restraint to prevent really quick polling? I think not, caveat programmator
	if c.PollInterval <= 0 {
		return errors.New("poll interval must be a positive duration")
	}

	return nil
}

// Subscriber makes SELECT queries on the chosen table with the interval defined in the config.
// The rows are unmarshaled into Watermill messages.
type Subscriber struct {
	config SubscriberConfig

	db *sql.DB

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool
}

func NewSubscriber(db *sql.DB, conf SubscriberConfig) (*Subscriber, error) {
	conf.setDefaults()
	err := conf.validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	sub := &Subscriber{
		config: conf,

		db: db,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),
	}

	return sub, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	// propagate the information about closing subscriber through ctx
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-s.closing
		cancel()
	}()

	out := make(chan *message.Message)
	var stmt *sql.Stmt

	// todo: this is hardly readable, maybe use text/template? complicated tho
	q := fmt.Sprintf(
		`SELECT %s FROM %s WHERE `+
			`TOPIC=? `+
			`AND %s.offset > (SELECT COALESCE(MAX(%s.offset), 0) FROM %s WHERE consumer_group=?) `+
			`ORDER BY %s.offset ASC LIMIT 1`,
		strings.Join(s.config.Unmarshaler.SelectColumns(), ","),
		s.config.MessagesTable,
		s.config.MessagesTable,
		s.config.OffsetsAckedTable,
		s.config.OffsetsAckedTable,
		s.config.MessagesTable,
	)

	s.config.Logger.Trace("Preparing query to select messages from db", watermill.LogFields{
		"q": q,
	})

	stmt, err = s.db.PrepareContext(
		ctx,
		q,
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare statement for SELECT")
	}

	s.subscribeWg.Add(1)
	go s.consume(ctx, stmt, topic, out)

	go func() {
		s.subscribeWg.Wait()
		close(out)
		if err := stmt.Close(); err != nil {
			s.config.Logger.Error("Could not close statement", err, nil)
		}
	}()

	return out, nil
}

func (s *Subscriber) consume(ctx context.Context, stmt *sql.Stmt, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()
	logger := s.config.Logger.With(watermill.LogFields{
		"topic": topic,
	})

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping consume, subscriber closing", nil)
			return
		default:
			// go on querying
		}

		row := stmt.QueryRowContext(
			ctx,
			topic,
			s.config.ConsumerGroup,
		)

		msg, err := s.config.Unmarshaler.Unmarshal(row)
		if err != nil && errors.Cause(err) == sql.ErrNoRows {
			// wait until polling for the next message
			time.Sleep(s.config.PollInterval)
			continue
		}
		if err != nil {
			logger.Error("Could not scan rows from query", err, nil)
			continue
		}

		s.sendMessage(ctx, msg, out, logger)
	}
}

// sendMessages sends messages on the output channel.
// whenever a message is successfully sent and acked, the message's index is sent of the offsetCh.
func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg MessageWithOffset,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) {

ResendLoop:
	for {
		logger = logger.With(watermill.LogFields{
			"msg_uuid": msg.UUID,
		})

		select {
		case out <- msg.Message:
		// message sent, go on
		case <-ctx.Done():
			logger.Info("Discarding queued message, subscriber closing", nil)
			return
		}

		select {
		case <-msg.Acked():
			logger.Debug("Message acked", nil)

			err := s.saveOffset(ctx, msg.Offset)
			if err != nil {
				logger.Error("Could not mark message as acked", err, nil)
			} else {
				logger.Debug("Saved msg offset to db", nil)
			}

			return

		case <-msg.Nacked():
			//message nacked, try resending
			logger.Debug("Message nacked, resending", nil)
			continue ResendLoop

		case <-ctx.Done():
			logger.Info("Discarding queued message, subscriber closing", nil)
			return
		}
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	if err := s.db.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Subscriber) saveOffset(ctx context.Context, offset int64) (err error) {
	var tx *sql.Tx
	tx, err = s.db.BeginTx(ctx, nil)
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

	q := fmt.Sprintf(
		`INSERT INTO %s (offset, consumer_group) VALUES (?, ?) ON DUPLICATE KEY UPDATE offset=VALUES(offset)`,
		s.config.OffsetsAckedTable,
	)

	s.config.Logger.Trace("Updating consumer group offset", watermill.LogFields{
		"q":              q,
		"consumer_group": s.config.ConsumerGroup,
	})

	_, err = tx.ExecContext(
		ctx,
		q,
		offset,
		s.config.ConsumerGroup,
	)

	return err
}
