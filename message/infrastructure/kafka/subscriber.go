package kafka

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
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
	Brokers         []string
	ConsumerGroup   string
	AutoOffsetReset string

	ConsumersCount      int
	CloseCheckThreshold time.Duration

	KafkaConfigOverwrite kafka.ConfigMap
}

func (c *SubscriberConfig) setDefaults() {
	if c.CloseCheckThreshold == 0 {
		c.CloseCheckThreshold = time.Second * 1
	}
	if c.ConsumersCount == 0 {
		c.ConsumersCount = runtime.NumCPU()
	}
	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = "earliest"
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.ConsumersCount <= 0 {
		return errors.Errorf("ConsumersCount must be greater than 0, have %d", c.ConsumersCount)
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
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),

		"auto.offset.reset":    config.AutoOffsetReset,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": config.AutoOffsetReset},

		"session.timeout.ms": 6000,

		"debug": ",",
	}

	if config.ConsumerGroup != "" {
		kafkaConfig.SetKey("group.id", config.ConsumerGroup)
		kafkaConfig.SetKey("enable.auto.commit", true)
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

// todo - review!!
func (s *confluentSubscriber) Subscribe(topic string) (chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	logFields := watermill.LogFields{
		"topic":                   topic,
		"kafka_subscribers_count": s.config.ConsumersCount,
		"consumer_group":          s.config.ConsumerGroup,
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	output := make(chan *message.Message, 0)

	s.allSubscribersWg.Add(1)

	consumersWg := &sync.WaitGroup{}

	for i := 0; i < s.config.ConsumersCount; i++ {
		consumer, err := s.consumerConstructor(s.config)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create consumer")
		}

		if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
			return nil, errors.Wrapf(err, "cannot subscribe topic %s", topic)
		}

		consumerLogFields := logFields.Add(watermill.LogFields{
			"consumer_no": i,
		})
		s.logger.Debug("Starting messages consumer", consumerLogFields)

		consumersWg.Add(1)
		go func(events chan<- *message.Message, consumer *kafka.Consumer) {
			defer func() {
				if err := consumer.Close(); err != nil {
					s.logger.Error("Cannot close consumer", err, consumerLogFields)
				}

				consumersWg.Done()
				s.logger.Debug("Messages consumption consumer done", consumerLogFields)
			}()

		EventsLoop:
			for {
				select {
				case <-s.closing:
					s.logger.Debug("Closing message consumer", consumerLogFields)
					return
				default:
					ev := consumer.Poll(100)
					if ev == nil {
						continue
					}

					switch e := ev.(type) {
					case *kafka.Message:
						if e.TopicPartition.Error != nil {
							s.logger.Error("Partition error", e.TopicPartition.Error, consumerLogFields)
							return
						}

						receivedMsgLogFields := consumerLogFields.Add(watermill.LogFields{
							"kafka_partition":        e.TopicPartition.Partition,
							"kafka_partition_offset": e.TopicPartition.Offset,
						})
						s.logger.Trace("Received message from Kafka", receivedMsgLogFields)

						msg, err := s.unmarshaler.Unmarshal(e)
						if err != nil {
							s.logger.Error("Cannot deserialize message", err, receivedMsgLogFields)
							continue EventsLoop
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
							s.rollback(consumer, e.TopicPartition)
							continue
						case events <- msg:
							// ok
						}

						s.logger.Trace("Waiting for ACK", receivedMsgLogFields)

					AckLoop:
						for {
							select {
							// todo - make it in cleaner way
							case <-msg.Acked():
								s.logger.Trace("Message acknowledged", receivedMsgLogFields)

								// todo - make it in cleaner way
								if s.config.ConsumerGroup != "" {
									stored, err := consumer.StoreOffsets([]kafka.TopicPartition{e.TopicPartition})
									if err != nil {
										s.logger.Error("Cannot store offsets", err, consumerLogFields)
										s.rollback(consumer, e.TopicPartition)
									} else {
										s.logger.Trace(
											"stored Kafka offsets",
											receivedMsgLogFields.Add(watermill.LogFields{"stored_offsets": stored}),
										)
									}
								}
								break AckLoop
							case <-msg.Nacked():
								s.logger.Info(
									"Nack sent",
									receivedMsgLogFields,
								)
								s.rollback(consumer, e.TopicPartition)
								break AckLoop
							case <-time.After(s.config.CloseCheckThreshold):
								s.logger.Info(
									fmt.Sprintf("Ack not received after %s", s.config.CloseCheckThreshold),
									receivedMsgLogFields,
								)
							}

							// check close
							select {
							case <-s.closing:
								s.logger.Info(
									"Ack not received and received close",
									receivedMsgLogFields,
								)
								s.rollback(consumer, e.TopicPartition) // todo - reactor occurance of rollback
								continue EventsLoop
							default:
								s.logger.Trace(
									"No close received, waiting for ACK",
									receivedMsgLogFields,
								)
							}
						}
					case kafka.PartitionEOF:
						s.logger.Trace("Reached end of partition", logFields)
					case kafka.OffsetsCommitted:
						s.logger.Trace("Offset committed", logFields.Add(watermill.LogFields{
							"offsets": e.String(),
						}))
					default:
						s.logger.Debug(
							"Unsupported msg",
							logFields.Add(watermill.LogFields{
								"msg":      fmt.Sprintf("%#v", e),
								"msg_type": fmt.Sprintf("%T", e),
							}),
						)
					}
				}
			}
		}(output, consumer)
	}

	go func() {
		consumersWg.Wait()
		s.logger.Debug("Closing message consumer", logFields)

		close(output)
		s.allSubscribersWg.Done()
	}()

	return output, nil
}

func (s *confluentSubscriber) rollback(consumer *kafka.Consumer, partition kafka.TopicPartition) {
	if err := consumer.Seek(partition, 1000*60); err != nil {
		// todo - how to handle?
		panic(err)
	}
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
