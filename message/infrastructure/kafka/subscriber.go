package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

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
) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if overwriteSaramaConfig == nil {
		overwriteSaramaConfig = DefaultSaramaSubscriberConfig()
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": shortuuid.New(),
	})

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

	InitializeTopicDetails *sarama.TopicDetail
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
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": shortuuid.New(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// we don't want to have buffered channel to not consume message from Kafka when consumer is not consuming
	output := make(chan *message.Message, 0)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		// blocking, until s.closing is closed
		s.handleReconnects(ctx, topic, output, consumeClosed, logFields)
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) handleReconnects(
	ctx context.Context,
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
		case <-ctx.Done():
			s.logger.Debug("Ctx cancelled, no reconnect needed", logFields)
			return
		default:
			// not closing
		}

		s.logger.Info("Reconnecting consumer", logFields)

		var err error
		consumeClosed, err = s.consumeMessages(ctx, topic, output, logFields)
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
	ctx context.Context,
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
		consumeMessagesClosed, err = s.consumeWithoutConsumerGroups(ctx, client, topic, output, logFields)
	} else {
		consumeMessagesClosed, err = s.consumeGroupMessages(ctx, client, topic, output, logFields)
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
	ctx context.Context,
	client sarama.Client,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (chan struct{}, error) {
	ctx, cancel := context.WithCancel(ctx)
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
		ctx:              ctx,
		messageHandler:   s.createMessagesHandler(output),
		logger:           s.logger,
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
	ctx context.Context,
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

		go s.consumePartition(ctx, partitionConsumer, messageHandler, partitionConsumersWg, logFields)
	}

	closed := make(chan struct{})
	go func() {
		partitionConsumersWg.Wait()
		close(closed)
	}()

	return closed, nil
}

func (s *Subscriber) consumePartition(
	ctx context.Context,
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
		case <-ctx.Done():
			return
		case kafkaMsg := <-kafkaMessages:
			if kafkaMsg == nil {
				// kafkaMessages is closed
				return
			}
			if err := messageHandler.processMessage(ctx, kafkaMsg, nil, logFields); err != nil {
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
	ctx              context.Context
	messageHandler   messageHandler
	logger           watermill.LoggerAdapter
	closing          chan struct{}
	messageLogFields watermill.LogFields
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kafkaMessages := claim.Messages()

	logFields := h.messageLogFields.Copy().Add(watermill.LogFields{
		"kafka_partition":      claim.Partition(),
		"kafka_initial_offset": claim.InitialOffset(),
	})

	h.logger.Debug("Consume claimed", logFields)

	for {
		select {
		case kafkaMsg := <-kafkaMessages:
			if kafkaMsg == nil {
				// kafkaMessages is closed
				return nil
			}
			if err := h.messageHandler.processMessage(h.ctx, kafkaMsg, sess, logFields); err != nil {
				// error will stop consumerGroupHandler
				return err
			}

		case <-h.closing:
			return nil

		case <-h.ctx.Done():
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
	ctx context.Context,
	kafkaMsg *sarama.ConsumerMessage,
	sess sarama.ConsumerGroupSession,
	messageLogFields watermill.LogFields,
) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"kafka_partition_offset": kafkaMsg.Offset,
		"kafka_partition":        kafkaMsg.Partition,
	})

	ctx = setPartitionToCtx(ctx, kafkaMsg.Partition)
	ctx = setPartitionOffsetToCtx(ctx, kafkaMsg.Offset)

	h.logger.Trace("Received message from Kafka", receivedMsgLogFields)

	msg, err := h.unmarshaler.Unmarshal(kafkaMsg)
	if err != nil {
		// resend will make no sense, stopping consumerGroupHandler
		return errors.Wrap(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

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

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	clusterAdmin, err := sarama.NewClusterAdmin(s.config.Brokers, s.saramaConfig)
	if err != nil {
		return errors.Wrap(err, "cannot create cluster admin")
	}
	defer func() {
		if closeErr := clusterAdmin.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	if err := clusterAdmin.CreateTopic(topic, s.config.InitializeTopicDetails, false); err != nil {
		return errors.Wrap(err, "cannot create topic")
	}

	s.logger.Info("Created Kafka topic", watermill.LogFields{"topic": topic})

	return nil
}

type PartitionOffset struct {
	Partition int32
	Offset    int64
}

func (s *Subscriber) PartitionOffsets(topic string) (offsets []PartitionOffset, err error) {
	client, err := sarama.NewClient(s.config.Brokers, s.saramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new Sarama client")
	}

	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get topic partitions")
	}

	for _, partition := range partitions {
		offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}
		if offset > 0 {
			// todo - doc why
			offset -= 1
		}

		offsets = append(offsets, PartitionOffset{
			Partition: partition,
			Offset:    offset,
		})
	}

	return offsets, nil
}
