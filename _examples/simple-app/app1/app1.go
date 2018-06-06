package main

import (
	"github.com/satori/go.uuid"
	"github.com/Pallinder/go-randomdata"
	"sync"
	"time"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/sarama"
	message2 "github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd/message/marshal"
	"github.com/renstrom/shortuuid"
	"github.com/roblaszczak/gooddd/message/handler/middleware"
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
	publisher, err := sarama.NewSimpleSyncProducer("test_topic", []string{"localhost:9092"}, marshal.Json)
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
		msg := message2.NewDefault(msgPayload.UUID(), msgPayload)

		middleware.SetCorrelationUUID(shortuuid.New(), msg)

		//fmt.Printf("Generated message: %#v\n", msg)

		wg.Add(1)
		go func() {
			// todo - how to create messages to send?
			err := publisher.Publish([]message2.Message{
				msg,
			})
			if err != nil {
				panic(err)
			}
			wg.Done()
		}()

		i--
		if i == 0 {
			break
		}
	}

	wg.Wait()
}
