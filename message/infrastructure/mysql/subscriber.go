package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/siddontang/go-mysql/driver"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

type SubscriberConfig struct {
	ConnectionConfig
	Offset      int64
	Unmarshaler Unmarshaler
	Logger      watermill.LoggerAdapter
	// PollInterval is the interval between subsequent SELECT queries. Defaults to 5s.
	PollInterval time.Duration
}

func (c *SubscriberConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
	if c.PollInterval == 0 {
		c.PollInterval = 5 * time.Second
	}
}

func (c SubscriberConfig) validate() error {
	if err := c.ConnectionConfig.validate(); err != nil {
		return err
	}

	if c.Offset < 0 {
		return errors.New("offset must be non-negative")
	}

	if c.Unmarshaler == nil {
		return errors.New("unmarshaler not set")
	}

	if c.PollInterval < 100*time.Millisecond {
		return errors.New("poll interval must be >100ms")
	}

	return nil
}

type Subscriber struct {
	config SubscriberConfig

	db *sql.DB

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool
}

func NewSubscriber(conf SubscriberConfig) (*Subscriber, error) {
	if err := conf.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	db, err := conf.connect()
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to mysql")
	}

	return &Subscriber{
		config: conf,
		db:     db,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),
	}, nil
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

	offset := s.config.Offset
	logger := s.config.Logger.With(watermill.LogFields{
		"topic": topic,
	})

	lock := sync.RWMutex{}

	offsetCh := make(chan int64)
	go func() {
		for offsetRead := range offsetCh {
			lock.RLock()
			if offsetRead > offset {
				lock.RUnlock()
				lock.Lock()
				offset = offsetRead
				lock.Unlock()
				continue
			}
			lock.RUnlock()
		}

		// todo: persist offset somewhere?
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

		selectArgs, err := s.config.Unmarshaler.ForSelect(offset, topic)
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

		messages, err := s.config.Unmarshaler.Unmarshal(rows)
		if err != nil {
			logger.Error("Could not unmarshal rows into messages", err, nil)
		}

		// todo: don't process the same message twice
		go s.sendMessages(ctx, messages, out, offsetCh, sendWg, logger)
	}

	sendWg.Wait()
	close(offsetCh)
}

// sendMessages sends messages on the output channel.
// whenever a message is successfully sent and acked, the message's index is sent of the offsetCh.
func (s *Subscriber) sendMessages(
	ctx context.Context,
	messages message.Messages,
	out chan *message.Message,
	offsetCh chan int64,
	sendWg *sync.WaitGroup,
	logger watermill.LoggerAdapter,
) {
	sendWg.Add(1)
	defer sendWg.Done()

	toSend := make(map[string]struct{}, len(messages))
	for _, msg := range messages {
		toSend[msg.UUID] = struct{}{}
	}

	for {
		if len(toSend) == 0 {
			// no more messages for resend
			break
		}

	MessageLoop:
		for _, msg := range messages {
			if _, ok := toSend[msg.UUID]; !ok {
				continue MessageLoop
			}

			logger = logger.With(watermill.LogFields{
				"msg_uuid": msg.UUID,
			})

			select {
			case out <- msg:
			// message sent, go on
			case <-ctx.Done():
				logger.Info("Discarding queued messages, subscriber closing", nil)
				return
			}

			select {
			case <-msg.Acked():
				// message acked, move to the next message
				delete(toSend, msg.UUID)
				continue MessageLoop
			case <-msg.Nacked():
				//message nacked, try resending
				continue MessageLoop
			case <-ctx.Done():
				logger.Info("Discarding queued messages, subscriber closing", nil)
				return
			}
		}
	}
}

// SetOffset sets the offset to begin with for each new Subscribe call
func (s *Subscriber) SetOffset(offset int64) error {
	if offset < 0 {
		return errors.New("offset must be non-negative")
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

	return s.db.Close()
}
