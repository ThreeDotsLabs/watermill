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
)

var confluentAckCloseCheckThreshold = time.Second * 5 // todo - config

type ConfluentConsumerConstructor func(brokers []string, consumerGroup string) (*kafka.Consumer, error)

type confluentSubscriber struct {
	brokers             []string
	consumerGroup       string
	unmarshaler        Unmarshaler
	consumerConstructor ConfluentConsumerConstructor
	logger              gooddd.LoggerAdapter

	closing          chan struct{}
	allSubscribersWg *sync.WaitGroup
	consumersCount   int

	closed bool
}

// todo - ubiquitous name: listener, consumer, subscriber
func NewConfluentSubscriber(brokers []string, consumerGroup string, unmarshaler Unmarshaler, logger gooddd.LoggerAdapter) (message.Subscriber) {
	return NewCustomConfluentSubscriber(brokers, consumerGroup, unmarshaler, DefaultConfluentConsumerConstructor, logger)
}

func NewCustomConfluentSubscriber(
	brokers []string,
	consumerGroup string,
	unmarshaler Unmarshaler,
	consumerConstructor ConfluentConsumerConstructor,
	logger gooddd.LoggerAdapter,
) (message.Subscriber) {
	return &confluentSubscriber{
		brokers:             brokers,
		consumerGroup:       consumerGroup,
		unmarshaler:        unmarshaler,
		consumerConstructor: consumerConstructor,
		logger:              logger,

		closing:          make(chan struct{}, 1),
		allSubscribersWg: &sync.WaitGroup{},
		consumersCount:   8, // todo - config
	}
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
		// todo - allow ssl? add flexability
	})

}

// todo - review!!
func (s *confluentSubscriber) Subscribe(topic string) (chan message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	logFields := gooddd.LogFields{
		"topic":                   topic,
		"kafka_subscribers_count": s.consumersCount,
		"consumer_group":          s.consumerGroup,
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	output := make(chan message.Message, 0)

	s.allSubscribersWg.Add(1)

	consumersWg := &sync.WaitGroup{}

	for i := 0; i < s.consumersCount; i++ {
		consumer, err := s.consumerConstructor(s.brokers, s.consumerGroup)
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
					// todo - handle err
					panic(err)
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
							// todo - handle
							panic(e.TopicPartition.Error)
						}

						receivedMsgLogFields := consumerLogFields.Add(gooddd.LogFields{
							"kafka_partition":        e.TopicPartition.Partition,
							"kafka_partition_offset": e.TopicPartition.Offset,
						})
						s.logger.Trace("Received message from Kafka", receivedMsgLogFields)

						msg, err := s.unmarshaler.Unmarshal(e)
						// todo - move it out?
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
									// todo - log
									s.logger.Info(
										"Message error from ACK",
										receivedMsgLogFields.Add(gooddd.LogFields{"err": err}),
									)
									s.rollback(consumer, e.TopicPartition)
								} else {
									s.logger.Trace("Message acknowledged", receivedMsgLogFields)
									stored, err := consumer.StoreOffsets([]kafka.TopicPartition{e.TopicPartition})
									if err != nil {
										// todo - handle
										panic(err)
									}
									s.logger.Trace(
										"stored Kafka offsets",
										receivedMsgLogFields.Add(gooddd.LogFields{"stored_offsets": stored}),
									)
								}
								break AckLoop
							case <-time.After(confluentAckCloseCheckThreshold):
								s.logger.Info(
									fmt.Sprintf("Ack not received after %s", confluentAckCloseCheckThreshold),
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
						s.logger.Info(
							"Unsupported msg",
							logFields.Add(gooddd.LogFields{"msg": fmt.Sprintf("%#v", s)}),
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
		// todo - handle
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

	// todo - errors from consumers?
	return nil
}
