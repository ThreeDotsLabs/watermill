// todo - move to handler level?
package confluent

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"fmt"
	"github.com/roblaszczak/gooddd/message"
	"sync"
	"github.com/roblaszczak/gooddd"
)

type confluentKafkaDeserializer func(kafka.Message) (*message.Message, error)

type confluentKafkaGroupGenerator func(subscriberMeta message.SubscriberMetadata) string

type unmarshalMessageFunc func(data []byte, msg *message.Message) error

type confluentKafka struct {
	deserializer   confluentKafkaDeserializer
	groupGenerator confluentKafkaGroupGenerator

	unmarshalMessage unmarshalMessageFunc

	closing chan struct{}

	allSubscribersWg *sync.WaitGroup

	logger gooddd.LoggerAdapter

	poolersCount int
}

func NewConfluentKafka(
	deserializer confluentKafkaDeserializer,
	groupGenerator confluentKafkaGroupGenerator,
	unmarshalMessageFunc unmarshalMessageFunc,
	logger gooddd.LoggerAdapter,
) (message.Subscriber) {
	return &confluentKafka{
		deserializer:   deserializer,
		groupGenerator: groupGenerator,

		unmarshalMessage: unmarshalMessageFunc,

		closing: make(chan struct{}),

		allSubscribersWg: &sync.WaitGroup{},

		logger: logger,

		poolersCount: 8, // todo - config
	}
}

func (s confluentKafka) createConsumer(consumerGroup string) (*kafka.Consumer, error) {
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id": consumerGroup,

		// todo ?
		"auto.offset.reset":    "earliest",
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},

		"session.timeout.ms": 6000,
		"enable.auto.commit": true,
		// todo - allow ssl? add flexability
	})

}

func (s confluentKafka) Subscribe(topic string, metadata message.SubscriberMetadata) (chan *message.Message, error) {
	consumerGroup := s.groupGenerator(metadata)

	logFields := gooddd.LogFields{
		"topic":                   topic,
		"subscriber_name":         metadata.SubscriberName,
		"kafka_subscribers_count": fmt.Sprintf("%d", s.poolersCount),
		"consumer_group":          consumerGroup,
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	consumer, err := s.createConsumer(consumerGroup)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create subscriber")
	}

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, errors.Wrapf(err, "cannot subscribe topic %s", topic)
	}

	output := make(chan *message.Message, 0)

	s.allSubscribersWg.Add(1)

	poolersWg := &sync.WaitGroup{}
	poolersWg.Add(s.poolersCount)

	go func() {
		poolersWg.Wait()
		s.logger.Debug("Closing message consumer", logFields)

		err := consumer.Close()
		if err != nil {
			// todo - handle err
			fmt.Println(err)
		}

		close(output)
		s.allSubscribersWg.Done()
	}()

	for i := 0; i < s.poolersCount; i++ {
		subscriberLogFields := logFields.Add(gooddd.LogFields{
			"pooler_no": i,
		})
		s.logger.Debug("Starting messages pooler", subscriberLogFields)

		go func(events chan<- *message.Message) {
			defer func() {
				defer poolersWg.Done()
				s.logger.Debug("Messages consumption done", subscriberLogFields)
			}()

			for {
				select {
				case <-s.closing:
					s.logger.Debug("Closing message pooler", subscriberLogFields)
					return
				default:
					ev := consumer.Poll(100)
					if ev == nil {
						continue
					}

					switch e := ev.(type) {
					case *kafka.Message:
						receivedMsgLogFields := subscriberLogFields.Add(gooddd.LogFields{
							"kafka_partition":        e.TopicPartition.Partition,
							"kafka_partition_offset": e.TopicPartition.Offset,
						})
						s.logger.Trace("Received message from Kafka", receivedMsgLogFields)

						// todo - wtf with it?
						msg, err := message.DefaultFactoryFunc(nil)
						if err != nil {
							fmt.Println(err)
							continue
						}
						// todo - move it out?
						if err := s.unmarshalMessage(e.Value, msg); err != nil {
							// todo - err support
							fmt.Println(err)
							continue
						}

						receivedMsgLogFields = receivedMsgLogFields.Add(gooddd.LogFields{
							"message_id": msg.UUID,
						})

						s.logger.Trace("Kafka message unmarshalled, sending to output", receivedMsgLogFields)

						// todo - replace with func call to avoid events loss
						events <- msg

						s.logger.Trace("Waiting for ACK", receivedMsgLogFields)
						<-msg.Acknowledged()
						s.logger.Trace("Message acknowledged", receivedMsgLogFields)
					case kafka.PartitionEOF:
						fmt.Printf("%% Reached %v\n", e)
					default:
						fmt.Println("unsupportted msg:", e)
					}
				}
			}
		}(output)
	}

	return output, nil
}

func (s confluentKafka) Close() error {
	go func() {
		for {
			s.closing <- struct{}{}
		}
	}()
	s.allSubscribersWg.Wait()

	// todo - errors from consumers?
	return nil
}
