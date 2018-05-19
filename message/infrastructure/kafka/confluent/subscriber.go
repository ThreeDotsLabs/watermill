// todo - move to handler level?
package confluent

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"fmt"
	"github.com/roblaszczak/gooddd/message"
	"sync"
)

type confluentKafkaDeserializer func(kafka.Message) (*message.Message, error)

type confluentKafkaGroupGenerator func(subscriberMeta message.SubscriberMetadata) string

type unmarshalMessageFunc func(data []byte, msg *message.Message) error

type confluentKafka struct {
	deserializer   confluentKafkaDeserializer
	groupGenerator confluentKafkaGroupGenerator

	unmarshalMessage unmarshalMessageFunc

	closing chan struct{}

	subscribersWg *sync.WaitGroup
}

func NewConfluentKafka(
	deserializer confluentKafkaDeserializer,
	groupGenerator confluentKafkaGroupGenerator,
	unmarshalMessageFunc unmarshalMessageFunc,
) (message.Subscriber) {
	return &confluentKafka{
		deserializer:   deserializer,
		groupGenerator: groupGenerator,

		unmarshalMessage: unmarshalMessageFunc,

		closing: make(chan struct{}),

		subscribersWg: &sync.WaitGroup{},
	}
}

func (s confluentKafka) createConsumer(subscriberMeta message.SubscriberMetadata) (*kafka.Consumer, error) {
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          s.groupGenerator(subscriberMeta),

		// todo ?
		"auto.offset.reset":    "earliest",
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},

		"session.timeout.ms": 6000,
		"enable.auto.commit": true,
		// todo - allow ssl? add flexability
	})

}

func (s confluentKafka) Subscribe(topic string, metadata message.SubscriberMetadata) (chan *message.Message, error) {
	consumer, err := s.createConsumer(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create subscriber")
	}

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, errors.Wrapf(err, "cannot subscribe topic %s", topic)
	}

	output := make(chan *message.Message)

	s.subscribersWg.Add(1)

	for i := 0; i < 8; i++ {
		go func(events chan<- *message.Message) {
			defer s.subscribersWg.Done()

			for {
				select {
				case <-s.closing:
					err := consumer.Close()
					if err != nil {
						// todo - handle err
						fmt.Println(err)
					}
					close(output)
					return
				default:
					ev := consumer.Poll(100)
					if ev == nil {
						continue
					}

					switch e := ev.(type) {
					case *kafka.Message:
						//msg, err := s.consumer.ReadMessage(time.Millisecond * 100)
						// todo - support error handling
						// todo - support close
						//if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == TimedOutErrorCode {
						//	continue
						//}

						//if err != nil {
						//	// todo - better support for errors
						//	fmt.Println("error:", err)
						//	continue
						//}

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

						// todo - replace with func call to avoid events loss
						events <- msg
						<-msg.Acknowledged()
						if err != nil {
							// todo - err support
							fmt.Println(err)
						}
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
	s.subscribersWg.Wait()

	// todo - errors from consumers?
	return nil
}
