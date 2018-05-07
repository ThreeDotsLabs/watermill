package eventlistener

import (
	"github.com/roblaszczak/gooddd/domain"
	"github.com/roblaszczak/gooddd/msghandler"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"encoding/json"
	"fmt"
	"os"
)

const (
	TimedOutErrorCode = -185
)

type confluentKafka struct {
	consumer *kafka.Consumer

	closing chan struct{}
}

func CreateConfluentKafkaListener(subscriberName string) (msghandler.EventsListener, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "todo_app-" + subscriberName + "_v13", // todo - refactor

		"auto.offset.reset":    "earliest",
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},


		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})
	if err != nil {
		panic(err)
	}

	listener, err := NewConfluentKafka(c)
	if err != nil {
		return nil, err
	}

	return listener, nil
}

func NewConfluentKafka(consumer *kafka.Consumer) (msghandler.EventsListener, error) {
	return confluentKafka{consumer, make(chan struct{}, 0)}, nil
}

func (s confluentKafka) Subscribe(topic string) (chan domain.Event, error) {
	if err := s.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, errors.Wrapf(err, "cannot subscribe topic %s", topic)
	}

	output := make(chan domain.Event)

	go func(events chan<- domain.Event) {
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

					event := domain.Event{}
					if err := json.Unmarshal(e.Value, &event); err != nil {
						fmt.Println(err)
						continue
					}

					// todo - better validate event
					if event.ID == "" {
						continue
					}

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
