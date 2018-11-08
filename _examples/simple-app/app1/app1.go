package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/renstrom/shortuuid"
	"github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/roblaszczak/gooddd/message/router/middleware"
	"github.com/satori/go.uuid"
)

type postAdded struct {
	EventID string `json:"event_id"`

	AggregateUUID []byte `json:"aggregate_uuid"`

	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Tags []string `json:"tags"`

	Content string `json:"content"`
}

func (p postAdded) UUID() string {
	return p.EventID
}

func (p postAdded) EventOccurredOn() time.Time {
	return p.OccurredOn
}

func (p postAdded) AggregateID() []byte {
	return p.AggregateUUID
}

func (postAdded) AggregateType() string {
	return "post"
}

func (postAdded) AggregateVersion() int {
	return -1
}

func main() {
	publisher, err := kafka.NewPublisher([]string{"localhost:9092"}, marshal.ConfluentKafka{})
	if err != nil {
		panic(err)
	}

	i := 100000
	wg := &sync.WaitGroup{}

	for {
		msgPayload := postAdded{
			EventID: uuid.NewV4().String(),

			OccurredOn: time.Now(),

			Author: randomdata.FullName(randomdata.RandomGender),
			Title:  randomdata.SillyName(),

			Tags: []string{
				randomdata.Adjective(),
				randomdata.Adjective(),
				randomdata.Noun(),
			},

			Content: randomdata.Paragraph(),
		}

		b, err := json.Marshal(msgPayload)
		if err != nil {
			panic(err)
		}
		msg := message.NewMessage(msgPayload.UUID(), b)

		middleware.SetCorrelationID(shortuuid.New(), msg)

		wg.Add(1)
		err = publisher.Publish("test_topic", msg)
		if err != nil {
			panic(err)
		}
		wg.Done()

		i--

		if i%1000 == 0 {
			fmt.Println("left ", i)
		}

		if i == 0 {
			break
		}
	}

	wg.Wait()
}
