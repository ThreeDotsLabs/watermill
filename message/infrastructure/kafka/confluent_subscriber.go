package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
	"sync"
	"github.com/roblaszczak/gooddd"
	"strings"
	"time"
	"fmt"
	"runtime"
)

type ConfluentConsumerConstructor func(brokers []string, consumerGroup string) (*kafka.Consumer, error)

type confluentSubscriber struct {
	config SubscriberConfig

	unmarshaler         Unmarshaler
	consumerConstructor ConfluentConsumerConstructor
	logger              gooddd.LoggerAdapter

	closing          chan struct{}
	allSubscribersWg *sync.WaitGroup

	closed bool
}

type SubscriberConfig struct {
	Brokers       []string
	ConsumerGroup string

	ConsumersCount int

	CloseCheckThreshold time.Duration
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.ConsumerGroup == "" {
		return errors.New("missing consumer group")
	}
	if c.ConsumersCount <= 0 {
		return errors.Errorf("ConsumersCount must be greater than 0, have %d", c.ConsumersCount)
	}

	return nil
}

func (c *SubscriberConfig) setDefaults() {
	if c.CloseCheckThreshold == 0 {
		c.CloseCheckThreshold = time.Second * 1
	}
	if c.ConsumersCount == 0 {
		c.ConsumersCount = runtime.NumCPU()
	}
}

func NewConfluentSubscriber(
	config SubscriberConfig,
	unmarshaler Unmarshaler,
	logger gooddd.LoggerAdapter,
) (message.Subscriber, error) {
	return NewCustomConfluentSubscriber(config, unmarshaler, DefaultConfluentConsumerConstructor, logger)
}

func NewCustomConfluentSubscriber(
	config SubscriberConfig,
	unmarshaler Unmarshaler,
	consumerConstructor ConfluentConsumerConstructor,
	logger gooddd.LoggerAdapter,
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

func DefaultConfluentConsumerConstructor(brokers []string, consumerGroup string) (*kafka.Consumer, error) {
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"group.id":          consumerGroup,

		"auto.offset.reset":    "earliest",
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},

		"session.timeout.ms":       6000,
		"enable.auto.commit":       true,
		"enable.auto.offset.store": false,
	})
}

// todo - review!!
func (s *confluentSubscriber) Subscribe(topic string) (chan message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	logFields := gooddd.LogFields{
		"topic":                   topic,
		"kafka_subscribers_count": s.config.ConsumersCount,
		"consumer_group":          s.config.ConsumerGroup,
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	output := make(chan message.Message, 0)

	s.allSubscribersWg.Add(1)

	consumersWg := &sync.WaitGroup{}

	for i := 0; i < s.config.ConsumersCount; i++ {
		consumer, err := s.consumerConstructor(s.config.Brokers, s.config.ConsumerGroup)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create consumer")
		}

		if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
			return nil, errors.Wrapf(err, "cannot subscribe topic %s", topic)
		}

		consumerLogFields := logFields.Add(gooddd.LogFields{
			"consumer_no": i,
		})
		s.logger.Debug("Starting messages consumer", consumerLogFields)

		consumersWg.Add(1)
		go func(events chan<- message.Message, consumer *kafka.Consumer) {
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

						receivedMsgLogFields := consumerLogFields.Add(gooddd.LogFields{
							"kafka_partition":        e.TopicPartition.Partition,
							"kafka_partition_offset": e.TopicPartition.Offset,
						})
						s.logger.Trace("Received message from Kafka", receivedMsgLogFields)

						msg, err := s.unmarshaler.Unmarshal(e)
						if err != nil {
							s.logger.Error("Cannot deserialize message", err, receivedMsgLogFields)
							continue EventsLoop
						}

						receivedMsgLogFields = receivedMsgLogFields.Add(gooddd.LogFields{
							"message_id": msg.UUID(),
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
							case err := <-msg.Acknowledged():
								if err != nil {
									s.logger.Info(
										"Message error from ACK",
										receivedMsgLogFields.Add(gooddd.LogFields{"err": err}),
									)
									s.rollback(consumer, e.TopicPartition)
								} else {
									s.logger.Trace("Message acknowledged", receivedMsgLogFields)

									stored, err := consumer.StoreOffsets([]kafka.TopicPartition{e.TopicPartition})
									if err != nil {
										s.logger.Error("Cannot store offsets", err, consumerLogFields)
										s.rollback(consumer, e.TopicPartition)
									} else {
										s.logger.Trace(
											"stored Kafka offsets",
											receivedMsgLogFields.Add(gooddd.LogFields{"stored_offsets": stored}),
										)
									}
								}
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
					default:
						s.logger.Debug(
							"Unsupported msg",
							logFields.Add(gooddd.LogFields{
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

func (s *confluentSubscriber) CloseSubscriber() error {
	go func() {
		for {
			s.closing <- struct{}{}
		}
	}()
	s.allSubscribersWg.Wait()
	s.closed = true

	return nil
}
