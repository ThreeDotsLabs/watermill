package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/kisielk/sqlstruct"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
	// OffsetLastSaved makes the subscriber resume from the last saved offset. If no offset was saved, start from 0.
	OffsetLastSaved Offset = -1
)

type Offset int64

func (o Offset) Valid() error {
	if o < 0 && o != OffsetLastSaved {
		return errors.New("offset must be non-negative or OffsetLastSaved")
	}

	return nil
}

type SubscriberConfig struct {
	// Table is the name of the table to read messages from. Defaults to `events`.
	Table string
	// OffsetsTable is the name of the sql table that stores offsets. Defaults to `subscriber_offsets`.
	// After reading, offset of the last read message is persisted for each consumer group.
	OffsetsTable string
	// Offset is the initial offset to read from. Must be non-negative or OffsetLastSaved.
	Offset Offset

	// Subscribe calls within the same consumer groups share
	ConsumerGroup string

	// OnlyOnce, if true, makes Subscriber process each message exactly once for each consumer group.
	// At startup, the processed UUIDs are restored from DB.
	// In runtime, it is stored in memory, and persited to DB on Close.
	OnlyOnce bool
	// ProcessedMessagesTable is the name of the table that stores the processed messages uuids.
	// Defaults to `processed_messages`.
	ProcessedMessagesTable string

	Unmarshaler Unmarshaler
	Logger      watermill.LoggerAdapter

	// PollInterval is the interval between subsequent SELECT queries. Defaults to 5s.
	PollInterval time.Duration
}

func (c *SubscriberConfig) setDefaults() {
	if c.Table == "" {
		c.Table = "events"
	}
	if c.OffsetsTable == "" {
		c.OffsetsTable = "subscriber_offsets"
	}
	if c.ProcessedMessagesTable == "" {
		c.ProcessedMessagesTable = "processed_messages"
	}
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
	if c.PollInterval == 0 {
		c.PollInterval = 5 * time.Second
	}
}

func (c SubscriberConfig) validate() error {
	if err := c.Offset.Valid(); err != nil {
		return err
	}

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

	db                *sql.DB
	processedMessages *sync.Map

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

		db:                db,
		processedMessages: &sync.Map{},

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),
	}

	if conf.OnlyOnce {
		// todo: configurable timeout?
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		err = sub.restoreProcessedMessages(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "could not restore processed messages from db")
		}
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
	stmt, err = s.db.PrepareContext(
		ctx,
		fmt.Sprintf(`SELECT * FROM %s WHERE idx > ? AND TOPIC=? ORDER BY idx ASC`, s.config.Table),
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

	offset := s.config.Offset
	var err error
	if offset == OffsetLastSaved {
		offset, err = s.restoreOffset(ctx, topic)
		if err != nil {
			logger.Error("could not restore last offset for topic", err, nil)
			// todo: continue with offset 0?
			return
		}
	}

	lock := sync.RWMutex{}

	offsetCh := make(chan Offset)
	go func() {
		for offsetRead := range offsetCh {
			lock.RLock()
			if offsetRead > offset {
				lock.RUnlock()
				lock.Lock()
				offset = offsetRead

				lock.Unlock()
				// todo: should offsets be per topic or per consumer?
				if persistErr := s.persistOffset(ctx, topic, offset); persistErr != nil {
					logger.Error("Could not persist current offset", persistErr, nil)
				}
				continue
			}

			lock.RUnlock()
		}
	}()

	sendWg := &sync.WaitGroup{}

ConsumeLoop:
	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping consume, subscriber closing", nil)
			break ConsumeLoop
		case <-time.After(s.config.PollInterval):
			// go on querying
		}

		selectArgs, err := s.config.Unmarshaler.ForSelect(int64(offset), topic)
		if err != nil {
			logger.Error("Obtained incorrect parameters for SELECT query", err, nil)
			continue
		}

		lock.RLock()
		rows, err := stmt.QueryContext(ctx, selectArgs.Idx, selectArgs.Topic)
		lock.RUnlock()
		if err != nil {
			logger.Error("SELECT query failed", err, nil)
			continue
		}

		scanned, err := s.scan(rows)
		if err != nil {
			logger.Error("Could not scan rows from query", err, nil)
			continue
		}

		for _, msg := range scanned {
			if s.messageAlreadyProcessed(msg) {
				logger.Debug("message already processed, skipping", nil)
				continue
			}
			s.markMessageProcessed(msg)

			offsetCh <- Offset(msg.Idx)
			watermillMsg, err := s.config.Unmarshaler.Unmarshal(msg)
			if err != nil {
				logger.Error("Could not scan rows from query", err, nil)
				continue
			}
			go s.sendMessage(ctx, watermillMsg, out, sendWg, logger)

		}
	}

	sendWg.Wait()
	close(offsetCh)
}

// sendMessages sends messages on the output channel.
// whenever a message is successfully sent and acked, the message's index is sent of the offsetCh.
func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	sendWg *sync.WaitGroup,
	logger watermill.LoggerAdapter,
) {
	sendWg.Add(1)
	defer sendWg.Done()

ResendLoop:
	for {
		logger = logger.With(watermill.LogFields{
			"msg_uuid": msg.UUID,
		})

		select {
		case out <- msg:
		// message sent, go on
		case <-ctx.Done():
			logger.Info("Discarding queued message, subscriber closing", nil)
			return
		}

		select {
		case <-msg.Acked():
			// message acked, move to the next message
			return
		case <-msg.Nacked():
			//message nacked, try resending
			continue ResendLoop
		case <-ctx.Done():
			logger.Info("Discarding queued message, subscriber closing", nil)
			return
		}
	}
}

func (s *Subscriber) scan(rows *sql.Rows) ([]dbTransport, error) {
	var messages []dbTransport
	var msg *dbTransport
	var err error

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	for rows.Next() {
		msg = new(dbTransport)
		scanErr := sqlstruct.Scan(msg, rows)
		if scanErr != nil {
			err = multierror.Append(err, scanErr)
		} else {
			messages = append(messages, *msg)
		}
	}

	return messages, err
}

// SetOffset sets the offset to begin with for each new Subscribe call.
func (s *Subscriber) SetOffset(offset Offset) error {
	if err := offset.Valid(); err != nil {
		return err
	}

	s.config.Offset = offset
	return nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	var err error
	// todo: configurable timeout?
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	if s.config.OnlyOnce {
		// todo: store periodically and not only on Close? could lose some messages
		if storeErr := s.storeProcessedMessages(ctx); storeErr != nil {
			err = multierror.Append(err, storeErr)
		}
	}

	if closeErr := s.db.Close(); closeErr != nil {
		err = multierror.Append(err, closeErr)
	}

	return err
}

// persistOffset saves the lastest offset for the topic.
// Separate consumer groups have separate offsets.
// If Subscribe is called with OffsetLastSaved, it resumes from the last saved offset.
func (s *Subscriber) persistOffset(ctx context.Context, topic string, offset Offset) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "could not begin tx")
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = multierror.Append(err, rollbackErr)
			}
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				err = multierror.Append(err, commitErr)
			}
		}
	}()

	_, err = s.db.ExecContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s (topic, offset, consumer_group) VALUES(?,?,?) ON DUPLICATE KEY UPDATE offset=VALUES(offset)",
			s.config.OffsetsTable,
		),
		topic,
		int64(offset),
		s.config.ConsumerGroup,
	)

	return err
}

func (s *Subscriber) restoreOffset(ctx context.Context, topic string) (Offset, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errors.Wrap(err, "could not begin tx")
	}

	// todo: is the transaction necessary? it is read-only
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = multierror.Append(err, rollbackErr)
			}
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				err = multierror.Append(err, commitErr)
			}
		}
	}()

	row := s.db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT offset FROM %s WHERE topic=? AND consumer_group=?`,
			s.config.OffsetsTable,
		),
		topic,
		s.config.ConsumerGroup,
	)

	var offset int64
	err = row.Scan(&offset)

	return Offset(offset), err
}

func (s *Subscriber) restoreProcessedMessages(ctx context.Context) error {
	rows, err := s.db.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT uuid FROM %s WHERE consumer_group=?`, s.config.ProcessedMessagesTable),
		s.config.ConsumerGroup,
	)

	if err != nil {
		return errors.Wrap(err, "could not query db for processed messages")
	}

	var uuid []byte
	for rows.Next() {
		scanErr := rows.Scan(&uuid)
		if scanErr != nil {
			err = multierror.Append(err, scanErr)
			continue
		}

		s.processedMessages.Store(string(uuid), struct{}{})
	}

	return err
}

func (s *Subscriber) storeProcessedMessages(ctx context.Context) (err error) {
	var tx *sql.Tx
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "could not begin tx")
	}

	// todo: could this be made into a function that changes the returned err? it occurs several times
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = multierror.Append(err, rollbackErr)
			}
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				err = multierror.Append(err, commitErr)
			}
		}
	}()

	stmt, err := tx.PrepareContext(
		ctx,
		fmt.Sprintf(
			`INSERT INTO %s (uuid, consumer_group) VALUES(?,?) ON DUPLICATE KEY UPDATE uuid=VALUES(uuid)`,
			s.config.ProcessedMessagesTable,
		),
	)
	if err != nil {
		return errors.Wrap(err, "could not prepare statement to insert processed messages")
	}

	s.processedMessages.Range(func(key, value interface{}) bool {
		k, ok := key.(string)
		if !ok {
			s.config.Logger.Info("processed messages key not a string", watermill.LogFields{
				"key": fmt.Sprintf("%+v", k),
			})
			return true
		}

		if s.config.ConsumerGroup == "" {
			_, err = stmt.ExecContext(ctx, []byte(k))
		} else {
			_, err = stmt.ExecContext(ctx, []byte(k), s.config.ConsumerGroup)
		}
		if err != nil {
			s.config.Logger.Error("could not insert processed message UUID", err, nil)
			return true
		}

		return true
	})

	return nil
}

func (s Subscriber) processedMessageKey(transport dbTransport) string {
	if s.config.ConsumerGroup == "" {
		return string(transport.UUID)
	}
	return string(transport.UUID) + ":" + s.config.ConsumerGroup
}

func (s *Subscriber) messageAlreadyProcessed(transport dbTransport) bool {
	if !s.config.OnlyOnce {
		return false
	}

	_, ok := s.processedMessages.Load(s.processedMessageKey(transport))
	return ok
}

func (s *Subscriber) markMessageProcessed(transport dbTransport) {
	if !s.config.OnlyOnce {
		return
	}

	s.processedMessages.Store(s.processedMessageKey(transport), struct{}{})
}
