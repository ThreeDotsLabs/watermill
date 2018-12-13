package nats

import (
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	internalSync "github.com/ThreeDotsLabs/watermill/internal/sync"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/go-nats-streaming"
	"github.com/pkg/errors"
)

type Subscriber struct {
	conn   stan.Conn
	logger watermill.LoggerAdapter

	config SubscriberConfig

	subs     []stan.Subscription
	subsLock sync.Mutex

	closed  bool
	closing chan struct{}

	outputsWg            sync.WaitGroup
	processingMessagesWg sync.WaitGroup
}

type SubscriberConfig struct {
	ClusterID   string
	ClientID    string
	QueueGroup  string // todo - validate?
	DurableName string

	SubscribersCount int
	CloseTimeout     time.Duration

	AckWaitTimeout time.Duration

	StanOptions             []stan.Option
	StanSubscriptionOptions []stan.SubscriptionOption

	Unmarshaler Unmarshaler
}

func (c *SubscriberConfig) setDefaults() {
	if c.SubscribersCount <= 0 {
		c.SubscribersCount = 1
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = time.Second * 30
	}

	c.StanSubscriptionOptions = append(
		c.StanSubscriptionOptions,
		stan.SetManualAckMode(), // manual AckMode is required to support acking/nacking by client
	)

	if c.AckWaitTimeout != 0 {
		c.StanSubscriptionOptions = append(c.StanSubscriptionOptions, stan.AckWait(c.AckWaitTimeout))
	}
	if c.DurableName != "" {
		c.StanSubscriptionOptions = append(c.StanSubscriptionOptions, stan.DurableName(c.DurableName))
	}
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()

	if config.Unmarshaler == nil {
		return nil, errors.New("SubscriberConfig.Unmarshaler cannot be empty")
	}

	conn, err := stan.Connect(config.ClusterID, config.ClientID, config.StanOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}

	return &Subscriber{
		conn:    conn,
		logger:  logger,
		config:  config,
		closing: make(chan struct{}),
	}, nil
}

func (s *Subscriber) Subscribe(topic string) (chan *message.Message, error) {
	output := make(chan *message.Message, 0)
	s.outputsWg.Add(1)

	go func() {
		<-s.closing
		s.processingMessagesWg.Wait()

		close(output)
		s.outputsWg.Done()
	}()

	for i := 0; i < s.config.SubscribersCount; i++ {
		subscriberLogFields := watermill.LogFields{
			"subscriber_num": i,
			"topic":          topic,
		}

		s.logger.Debug("Starting subscriber", subscriberLogFields)

		sub, err := s.conn.QueueSubscribe(
			topic,
			s.config.QueueGroup,
			func(m *stan.Msg) {
				s.processingMessagesWg.Add(1)
				defer s.processingMessagesWg.Done()

				s.processMessage(m, output, subscriberLogFields)
			},
			s.config.StanSubscriptionOptions...,
		)
		if err != nil {
			return nil, errors.Wrap(err, "cannot subscribe")
		}

		s.subsLock.Lock()
		s.subs = append(s.subs, sub)
		s.subsLock.Unlock()
	}

	return output, nil
}

func (s *Subscriber) processMessage(m *stan.Msg, output chan *message.Message, logFields watermill.LogFields) {
	if s.closed {
		return
	}

	s.logger.Trace("Received message", logFields)

	msg, err := s.config.Unmarshaler.Unmarshal(m)
	if err != nil {
		s.logger.Error("Cannot unmarshal message", err, logFields)
		return
	}

	messageLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.logger.Trace("Unmarshaled message", messageLogFields)

	select {
	case output <- msg:
		s.logger.Trace("Message sent to consumer", messageLogFields)
	case <-s.closing:
		s.logger.Trace("Closing, message discarded", messageLogFields)
		return
	}

	select {
	case <-msg.Acked():
		if err := m.Ack(); err != nil {
			s.logger.Error("Cannot send ack", err, messageLogFields)
		}
		s.logger.Trace("Message Acked", messageLogFields)
	case <-msg.Nacked():
		s.logger.Trace("Message Nacked", messageLogFields)
		return
	case <-s.closing:
		s.logger.Trace("Closing, message discarded before ack", messageLogFields)
		return
	}
}

func (s *Subscriber) Close() error {
	s.subsLock.Lock()
	defer s.subsLock.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	s.logger.Debug("Closing subscriber", nil)
	defer s.logger.Debug("Subscriber closed", nil)

	var result error
	for _, sub := range s.subs {
		if err := sub.Close(); err != nil {
			result = multierror.Append(result, errors.Wrap(err, "cannot close sub"))
		}
	}

	if err := s.conn.Close(); err != nil {
		result = multierror.Append(result, errors.Wrap(err, "cannot close conn"))
	}

	close(s.closing)
	internalSync.WaitGroupTimeout(&s.outputsWg, s.config.CloseTimeout)

	return result
}
