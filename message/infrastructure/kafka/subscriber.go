package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/renstrom/shortuuid"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/pkg/errors"
)

type Subscriber struct {
	config       SubscriberConfig
	saramaConfig *sarama.Config

	unmarshaler Unmarshaler
	logger      watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed bool
}

// NewSubscriber creates a new Kafka Subscriber.
func NewSubscriber(
	config SubscriberConfig,
	overwriteSaramaConfig *sarama.Config,
	unmarshaler Unmarshaler,
	logger watermill.LoggerAdapter,
) (message.Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if overwriteSaramaConfig == nil {
		overwriteSaramaConfig = DefaultSaramaSubscriberConfig()
	}

	return &Subscriber{
		config:       config,
		saramaConfig: overwriteSaramaConfig,

		unmarshaler: unmarshaler,
		logger:      logger,

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Kafka consumer group.
	// When empty, all messages from all partitions will be returned.
	ConsumerGroup string

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration
}

// NoSleep can be set to SubscriberConfig.NackResendSleep and SubscriberConfig.ReconnectRetrySleep.
const NoSleep time.Duration = -1

func (c *SubscriberConfig) setDefaults() {
	if c.NackResendSleep == 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}

	return nil
}

// Subscribe subscribers for messages in Kafka.
//
// There are multiple subscribers spawned
func (s *Subscriber) Subscribe(topic string) (chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":       "kafka",
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
		"subscribe_uuid": shortuuid.New(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// we don't want to have buffered channel to not consume message from Kafka when consumer is not consuming
	output := make(chan *message.Message, 0)

	consumeClosed, err := s.consumeMessages(topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		defer s.subscribersWg.Done()
		s.handleReconnects(topic, output, consumeClosed, logFields)
	}()

	return output, nil
}

func (s *Subscriber) handleReconnects(
	topic string,
	output chan *message.Message,
	consumeClosed chan struct{},
	logFields watermill.LogFields,
) {
	for {
		// nil channel will cause deadlock
		if consumeClosed != nil {
			<-consumeClosed
		}

		s.logger.Info("consumeMessages stopped", logFields)

		select {
		// it's important to don't exit before consumeClosed,
		// to not trigger s.subscribersWg.Done() before consumer is closed
		case <-s.closing:
			s.logger.Debug("Closing subscriber, no reconnect needed", logFields)
			return
		default:
			// not closing
		}

		s.logger.Info("Reconnecting consumer", logFields)

		var err error
		consumeClosed, err = s.consumeMessages(topic, output, logFields)
		if err != nil {
			s.logger.Error("Cannot reconnect messages consumer", err, logFields)

			if s.config.ReconnectRetrySleep != NoSleep {
				time.Sleep(s.config.ReconnectRetrySleep)
			}
			continue
		}
	}
}

func (s *Subscriber) consumeMessages(
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (consumeMessagesClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	// Start with a client
	client, err := sarama.NewClient(s.config.Brokers, s.saramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new Sarama client")
	}

	if s.config.ConsumerGroup == "" {
		consumeMessagesClosed, err = s.consumeWithoutConsumerGroups(client, topic, output, logFields)
	} else {
		consumeMessagesClosed, err = s.consumeGroupMessages(client, topic, output, logFields)
	}

	go func() {
		<-consumeMessagesClosed
		if err := client.Close(); err != nil {
			s.logger.Error("Cannot close client", err, logFields)
		}
	}()

	return consumeMessagesClosed, err
}

func (s *Subscriber) consumeGroupMessages(
	client sarama.Client,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (chan struct{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-s.closing
		cancel()
	}()

	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(s.config.ConsumerGroup, client)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create consumer group client")
	}
	go func() {
		for err := range group.Errors() {
			s.logger.Error("Sarama internal error", err, logFields)
		}
	}()

	handler := consumerGroupHandler{
		messageHandler:   s.createMessagesHandler(output),
		closing:          s.closing,
		messageLogFields: logFields,
	}

	closed := make(chan struct{})
	go func() {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil && err != sarama.ErrUnknown {
			s.logger.Error("Group consume error", err, logFields)
		}

		if err := group.Close(); err != nil {
			s.logger.Error("Cannot close group client", err, logFields)
		}

		s.logger.Info("Consuming done", logFields)
		close(closed)
	}()

	return closed, nil
}

func (s *Subscriber) consumeWithoutConsumerGroups(
	client sarama.Client,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (chan struct{}, error) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create client")
	}

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get partitions")
	}

	partitionConsumersWg := &sync.WaitGroup{}

	for _, partition := range partitions {
		partitionConsumersWg.Add(1)

		partitionConsumer, err := consumer.ConsumePartition(topic, partition, s.saramaConfig.Consumer.Offsets.Initial)
		if err != nil {
			if err := client.Close(); err != nil && err != sarama.ErrClosedClient {
				s.logger.Error("Cannot close client", err, logFields)
			}
			return nil, errors.Wrap(err, "failed to start consumer for partition")
		}

		messageHandler := s.createMessagesHandler(output)

		go s.consumePartition(partitionConsumer, messageHandler, partitionConsumersWg, logFields)
	}

	closed := make(chan struct{})
	go func() {
		partitionConsumersWg.Wait()
		close(closed)
	}()

	return closed, nil
}

func (s *Subscriber) consumePartition(
	partitionConsumer sarama.PartitionConsumer,
	messageHandler messageHandler,
	partitionConsumersWg *sync.WaitGroup,
	logFields watermill.LogFields,
) {
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			s.logger.Error("Cannot close partition consumer", err, logFields)
		}
		partitionConsumersWg.Done()
	}()

	kafkaMessages := partitionConsumer.Messages()

	for {
		select {
		case <-s.closing:
			return
		case kafkaMsg := <-kafkaMessages:
			if kafkaMsg == nil {
				// kafkaMessages is closed
				return
			}
			if err := messageHandler.processMessage(kafkaMsg, nil, logFields); err != nil {
				return
			}
		}
	}
}

func (s *Subscriber) createMessagesHandler(output chan *message.Message) messageHandler {
	return messageHandler{
		outputChannel:   output,
		unmarshaler:     s.unmarshaler,
		nackResendSleep: s.config.NackResendSleep,
		logger:          s.logger,
		closing:         s.closing,
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	s.logger.Debug("Kafka subscriber closed", nil)

	return nil
}

type consumerGroupHandler struct {
	messageHandler   messageHandler
	closing          chan struct{}
	messageLogFields watermill.LogFields
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kafkaMessages := claim.Messages()

	for {
		select {
		case kafkaMsg := <-kafkaMessages:
			if kafkaMsg == nil {
				// kafkaMessages is closed
				return nil
			}
			if err := h.messageHandler.processMessage(kafkaMsg, sess, h.messageLogFields); err != nil {
				// error will stop consumerGroupHandler
				return err
			}

		case <-h.closing:
			return nil
		}
	}
}

type messageHandler struct {
	outputChannel chan<- *message.Message
	unmarshaler   Unmarshaler

	nackResendSleep time.Duration

	logger  watermill.LoggerAdapter
	closing chan struct{}
}

func (h messageHandler) processMessage(
	kafkaMsg *sarama.ConsumerMessage,
	sess sarama.ConsumerGroupSession,
	messageLogFields watermill.LogFields,
) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"kafka_partition":        kafkaMsg.Partition,
		"kafka_partition_offset": kafkaMsg.Offset,
	})

	h.logger.Trace("Received message from Kafka", receivedMsgLogFields)

	msg, err := h.unmarshaler.Unmarshal(kafkaMsg)
	if err != nil {
		// resend will make no sense, stopping consumerGroupHandler
		return errors.Wrap(err, "message unmarshal failed")
	}

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

ResendLoop:
	for {
		select {
		case h.outputChannel <- msg:
			h.logger.Trace("Message sent to consumer", receivedMsgLogFields)
		case <-h.closing:
			h.logger.Trace("Closing, message discarded", receivedMsgLogFields)
			return nil
		}

		select {
		case <-msg.Acked():
			if sess != nil {
				sess.MarkMessage(kafkaMsg, "")
			}
			h.logger.Trace("Message Acked", receivedMsgLogFields)
			break ResendLoop
		case <-msg.Nacked():
			h.logger.Trace("Message Nacked", receivedMsgLogFields)

			// reset acks, etc.
			msg = msg.Copy()
			if h.nackResendSleep != NoSleep {
				time.Sleep(h.nackResendSleep)
			}

			continue ResendLoop
		case <-h.closing:
			h.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
			return nil
		}
	}

	return nil
}
