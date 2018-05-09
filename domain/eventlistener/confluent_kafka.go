// todo - move to handler level?
package eventlistener

import (
	"github.com/roblaszczak/gooddd/handler"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"encoding/json"
	"fmt"
	"os"
)

type confluentKafkaDeserializer func(kafka.Message) (handler.Message, error)

type confluentKafkaGroupGenerator func(subscriberMeta handler.SubscriberMetadata) string

type ConfluentKafkaFactory struct {
	deserializer   confluentKafkaDeserializer
	groupGenerator confluentKafkaGroupGenerator
}

func NewConfluentKafkaFactory(
	deserializer confluentKafkaDeserializer,
	groupGenerator confluentKafkaGroupGenerator,
) ConfluentKafkaFactory {
	return ConfluentKafkaFactory{deserializer, groupGenerator}
}

func (f ConfluentKafkaFactory) CreateListener(subscriberMeta handler.SubscriberMetadata) (handler.MessageListener, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          f.groupGenerator(subscriberMeta),

		"auto.offset.reset":    "earliest",
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},

		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})
	if err != nil {
		panic(err)
	}

	listener, err := NewConfluentKafka(c, f.deserializer)
	if err != nil {
		return nil, err
	}

	return listener, nil
}

type confluentKafka struct {
	consumer     *kafka.Consumer
	deserializer confluentKafkaDeserializer

	closing chan struct{}
}

func NewConfluentKafka(consumer *kafka.Consumer, deserializer confluentKafkaDeserializer) (handler.MessageListener, error) {
	return confluentKafka{
		consumer,
		deserializer,
		make(chan struct{}, 0),
	}, nil
}

func (s confluentKafka) Subscribe(topic string) (chan handler.Message, error) {
	if err := s.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, errors.Wrapf(err, "cannot subscribe topic %s", topic)
	}

	output := make(chan handler.Message)

	go func(events chan<- handler.Message) {
		run := true

		for run {
			select {
			case <-s.closing:
				close(output)
				return
			case ev := <-s.consumer.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					s.consumer.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					s.consumer.Unassign()
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

					event := handler.Message{}
					if err := json.Unmarshal(e.Value, &event); err != nil {
						fmt.Println(err)
						continue
					}

					// todo - better validate event
					//if event.Payload == "" {
					//	continue
					//}

					//fmt.Printf("unmarshaled: %s \nto %#v\n\n", e.Value, event)

					events <- event
				case kafka.PartitionEOF:
					fmt.Printf("%% Reached %v\n", e)
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					run = false
				}
			}
		}
	}(output)

	return output, nil
}

func (s confluentKafka) Close() error {
	s.closing <- struct{}{}
	return s.consumer.Close()
}
