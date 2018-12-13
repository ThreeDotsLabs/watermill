package kafka

import (
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type ConfluentConsumerConstructor func(config SubscriberConfig) (*kafka.Consumer, error)

type confluentSubscriber struct {
	config SubscriberConfig

	unmarshaler         Unmarshaler
	consumerConstructor ConfluentConsumerConstructor
	logger              watermill.LoggerAdapter

	closing          chan struct{}
	allSubscribersWg *sync.WaitGroup

	closed bool
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Kafka consumer group.
	ConsumerGroup string
	// When we want to consume without consumer group, you should set it to true.
	// In practice you will receive all messages sent to the topic.
	NoConsumerGroup bool

	// Action to take when there is no initial offset in offset store or the desired offset is out of range.
	// Available options: smallest, earliest, beginning, largest, latest, end, error.
	AutoOffsetReset string

	// How much consumers should be spawned.
	// Every consumer will receive messages for their own partition so messages order will be preserved.
	ConsumersCount int

	// Passing librdkafka options.
	// Available options: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	KafkaConfigOverwrite kafka.ConfigMap
}

func (c *SubscriberConfig) setDefaults() {
	if c.ConsumersCount == 0 {
		c.ConsumersCount = runtime.NumCPU()
	}
	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = "latest"
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.ConsumersCount <= 0 {
		return errors.Errorf("ConsumersCount must be greater than 0, have %d", c.ConsumersCount)
	}
	if c.ConsumerGroup == "" && !c.NoConsumerGroup {
		return errors.New("NoConsumerGroup must be true or ConsumerGroup must be set")
	}

	return nil
}

func NewConfluentSubscriber(
	config SubscriberConfig,
	unmarshaler Unmarshaler,
	logger watermill.LoggerAdapter,
) (message.Subscriber, error) {
	return NewCustomConfluentSubscriber(config, unmarshaler, DefaultConfluentConsumerConstructor, logger)
}

func NewCustomConfluentSubscriber(
	config SubscriberConfig,
	unmarshaler Unmarshaler,
	consumerConstructor ConfluentConsumerConstructor,
	logger watermill.LoggerAdapter,
) (message.Subscriber, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &confluentSubscriber{
		config:              config,
		unmarshaler:         unmarshaler,
		consumerConstructor: consumerConstructor,
		logger:              logger,

		closing:          make(chan struct{}, 1),
		allSubscribersWg: &sync.WaitGroup{},
	}, nil
}

func DefaultConfluentConsumerConstructor(config SubscriberConfig) (*kafka.Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),

		"auto.offset.reset":    config.AutoOffsetReset,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": config.AutoOffsetReset},

		"session.timeout.ms": 6000,

		"debug": ",",
	}

	if !config.NoConsumerGroup {
		kafkaConfig.SetKey("group.id", config.ConsumerGroup)
		kafkaConfig.SetKey("enable.auto.commit", true)

		// to achieve at-least-once delivery we store offsets after processing of the message
		kafkaConfig.SetKey("enable.auto.offset.store", false)

	} else {
		// this group will be not committed, setting just for api requirements
		kafkaConfig.SetKey("group.id", "no_group_"+uuid.NewV4().String())
		kafkaConfig.SetKey("enable.auto.commit", false)
	}

	if err := mergeConfluentConfigs(kafkaConfig, config.KafkaConfigOverwrite); err != nil {
		return nil, err
	}

	return kafka.NewConsumer(kafkaConfig)
}

// Subscribe subscribers for messages in Kafka.
//
// They are multiple subscribers spawned
func (s *confluentSubscriber) Subscribe(topic string) (chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	logFields := watermill.LogFields{
		"provider":                ProviderName,
		"topic":                   topic,
		"kafka_subscribers_count": s.config.ConsumersCount,
		"consumer_group":          s.config.ConsumerGroup,
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// we don't want to have buffered channel to not consume message from Kafka when consumer is not consuming
	output := make(chan *message.Message, 0)

	s.allSubscribersWg.Add(1)

	subscribersWg := &sync.WaitGroup{}

	for i := 0; i < s.config.ConsumersCount; i++ {
		kafkaConsumer, err := s.createConsumer(topic)
		if err != nil {
			return nil, err
		}

		subscribersWg.Add(1)
		go func(i int) {
			(&consumer{
				config:   s.config,
				consumer: kafkaConsumer,
				consumerLogFields: logFields.Add(watermill.LogFields{
					"consumer_no": i,
				}),
				outputChannel: output,
				unmarshaler:   s.unmarshaler,
				closing:       s.closing,
				logger:        s.logger,
			}).consumeMessages()

			subscribersWg.Done()
		}(i)
	}

	go func() {
		subscribersWg.Wait()
		s.logger.Debug("Closing message consumer", logFields)

		close(output)
		s.allSubscribersWg.Done()
	}()

	return output, nil
}

func (s *confluentSubscriber) createConsumer(topic string) (*kafka.Consumer, error) {
	consumer, err := s.consumerConstructor(s.config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create consumer")
	}

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, errors.Wrapf(err, "cannot subscribe topic %s", topic)
	}

	return consumer, nil
}

func (s *confluentSubscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.allSubscribersWg.Wait()

	s.logger.Debug("Kafka subscriber closed", nil)

	return nil
}

var (
	ErrClosingConsumer = errors.New("closing subscriber")
	ErrNackReceived    = errors.New("closing subscriber")
)

type consumer struct {
	config SubscriberConfig

	consumer          *kafka.Consumer
	consumerLogFields watermill.LogFields
	outputChannel     chan<- *message.Message

	unmarshaler Unmarshaler
	closing     chan struct{}

	logger watermill.LoggerAdapter
}

func (s *consumer) consumeMessages() {
	s.logger.Debug("Starting messages consumer", s.consumerLogFields)

	defer func() {
		if err := s.consumer.Close(); err != nil {
			s.logger.Error("Cannot close consumer", err, s.consumerLogFields)
		}

		s.logger.Debug("Messages consumption consumer done", s.consumerLogFields)
	}()

MessagesLoop:
	for {
		select {
		case <-s.closing:
			s.logger.Debug("Closing message consumer", s.consumerLogFields)
			break MessagesLoop
		default:
			ev := s.consumer.Poll(100)
			if ev == nil {
				continue MessagesLoop
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if err := s.handleMessage(e); err != nil {
					if err := s.handleMessageError(e, err); err != nil {
						// something went really wrong, let's die
						// todo - better handling strategy
						s.logger.Error("Handling message error failed, stopping consumer", err, s.consumerLogFields)
						break MessagesLoop
					}
				}
			case kafka.PartitionEOF:
				s.logger.Trace("Reached end of partition", s.consumerLogFields)
			case kafka.OffsetsCommitted:
				s.logger.Trace("Offset committed", s.consumerLogFields.Add(watermill.LogFields{
					"offsets": e.String(),
				}))
			default:
				s.logger.Debug(
					"Unsupported msg",
					s.consumerLogFields.Add(watermill.LogFields{
						"msg":      fmt.Sprintf("%#v", e),
						"msg_type": fmt.Sprintf("%T", e),
					}),
				)
			}
		}
	}
}

func (s *consumer) handleMessage(e *kafka.Message) error {
	if e.TopicPartition.Error != nil {
		return e.TopicPartition.Error
	}

	receivedMsgLogFields := s.consumerLogFields.Add(watermill.LogFields{
		"kafka_partition":        e.TopicPartition.Partition,
		"kafka_partition_offset": e.TopicPartition.Offset,
	})
	s.logger.Trace("Received message from Kafka", receivedMsgLogFields)

	msg, err := s.unmarshaler.Unmarshal(e)
	if err != nil {
		return err
	}

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

	s.logger.Trace("Kafka message unmarshalled, sending to output", receivedMsgLogFields)

	select {
	case <-s.closing:
		s.logger.Info(
			"Message not sent and close received, making rollback",
			receivedMsgLogFields,
		)
		return ErrClosingConsumer
	case s.outputChannel <- msg:
		// message consumed, waiting for ack
	}

	s.logger.Trace("Waiting for ACK", receivedMsgLogFields)
	if err := s.waitForAck(msg, receivedMsgLogFields); err != nil {
		return err
	}

	if err := s.storeOffsets(e, receivedMsgLogFields); err != nil {
		return err
	}

	return nil
}

func (s *consumer) handleMessageError(kafkaMessage *kafka.Message, err error) error {
	switch err {
	case ErrNackReceived:
		s.logger.Debug("Nack received", s.consumerLogFields)
	case ErrClosingConsumer:
		s.logger.Debug("Close received during processing message", s.consumerLogFields)
	default:
		s.logger.Error("Error during handling message", err, s.consumerLogFields)
	}

	if err := s.rollback(kafkaMessage); err != nil {
		return errors.Wrap(err, "rollback failed")
	}

	return nil
}

func (s *consumer) waitForAck(msg *message.Message, receivedMsgLogFields watermill.LogFields) error {
	select {
	case <-msg.Acked():
		s.logger.Trace("Message acknowledged", receivedMsgLogFields)
		return nil
	case <-msg.Nacked():
		s.logger.Info(
			"Nack sent",
			receivedMsgLogFields,
		)
		return ErrNackReceived
	case <-s.closing:
		s.logger.Info(
			"Ack not received and received close",
			receivedMsgLogFields,
		)
		return ErrClosingConsumer
	}
}

func (s *consumer) storeOffsets(kafkaMessage *kafka.Message, receivedMsgLogFields watermill.LogFields) error {
	if s.config.ConsumerGroup == "" {
		// if we have no consumer group, it is not sense to store offsets
		return nil
	}

	stored, err := s.consumer.StoreOffsets([]kafka.TopicPartition{kafkaMessage.TopicPartition})
	if err != nil {
		return errors.Wrap(err, "cannot store offsets")
	}

	s.logger.Trace(
		"Stored Kafka offsets",
		receivedMsgLogFields.Add(watermill.LogFields{"stored_offsets": stored}),
	)

	return nil
}

func (s *consumer) rollback(kafkaMessage *kafka.Message) error {
	return s.consumer.Seek(kafkaMessage.TopicPartition, 1000*60)
}
