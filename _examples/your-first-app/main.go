package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	brokers      = []string{"kafka:9092"}
	consumeTopic = "your-first-app_events"
	publishTopic = "your-first-app_events-processed"

	logger = watermill.NewStdLogger(
		true,  // debug
		false, // trace
	)
	marshaler = kafka.DefaultMarshaler{}
)

// createPublisher is a helper function which creates Publisher, in this case - Kafka Publisher.
// It is based on `confluent-kafka-go` which requires rdkafka installed.
func createPublisher() message.Publisher {
	kafkaPublisher, err := kafka.NewPublisher(
		brokers,
		marshaler,
		nil,
		logger,
	)
	if err != nil {
		panic(err)
	}

	return kafkaPublisher
}

// createSubscriber is helper function as previous, but in this case creates Subscriber.
func createSubscriber(consumerGroup string) message.Subscriber {
	kafkaSubscriber, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:       brokers,
		ConsumerGroup: consumerGroup, // every handler will have separated consumer group
	}, nil, marshaler, logger)
	if err != nil {
		panic(err)
	}

	return kafkaSubscriber
}

type event struct {
	Num int `json:"num"`
}

// publishEvents which will produce some events for consuming.
func publishEvents(publisher message.Publisher) {
	i := 0
	for {
		payload, err := json.Marshal(event{Num: int(time.Now().Unix())})
		if err != nil {
			panic(err)
		}

		err = publisher.Publish(consumeTopic, message.NewMessage(
			watermill.NewUUID(), // uuid of the message, very useful for debugging
			payload,
		))
		if err != nil {
			panic(err)
		}

		i++
		time.Sleep(time.Second)
	}
}

func main() {
	publisher := createPublisher()

	// producing events in background
	go publishEvents(publisher)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	// Consumer is created with consumer group handler_1
	subscriber := createSubscriber("handler_1")

	// adding handler, multiple handlers can be added
	router.AddHandler(
		"handler_1",  // handler name, must be unique
		consumeTopic, // topic from which messages should be consumed
		subscriber,
		publishTopic, // topic to which produced messages should be published
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			consumedPayload := event{}
			err := json.Unmarshal(msg.Payload, &consumedPayload)
			if err != nil {
				// default behavior when handler returns error is sending Nack (negative-acknowledgement)
				// the message will be processed again
				//
				// you can change default behaviour by using for example middleware.Retry or middleware.PoisonQueue
				// you can also implement your own
				return nil, err
			}

			log.Printf("received event %d", consumedPayload.Num)

			type processedEvent struct {
				EventNum int       `json:"event_num"`
				Time     time.Time `json:"time"`
			}
			producedPayload, err := json.Marshal(processedEvent{
				EventNum: consumedPayload.Num,
				Time:     time.Now(),
			})
			if err != nil {
				return nil, err
			}

			producedMessage := message.NewMessage(watermill.NewUUID(), producedPayload)

			return []*message.Message{producedMessage}, nil
		},
	)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}
