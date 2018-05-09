package main

import (
	"github.com/satori/go.uuid"
	"github.com/Pallinder/go-randomdata"
	"github.com/roblaszczak/gooddd/domain/eventstore"
	"sync"
	"time"
	"github.com/roblaszczak/gooddd/domain"
)

type postAdded struct {
	UUID          []byte `json:"uuid"`
	AggregateUUID []byte `json:"aggregate_uuid"`

	OccurredOn time.Time `json:"occurred_on"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Tags []string `json:"tags"`

	Content string `json:"content"`
}

func (p postAdded) EventID() []byte {
	return p.UUID
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
	es, err := eventstore.NewSimpleSyncKafka([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	i := 10
	wg := &sync.WaitGroup{}

	for {
		message := postAdded{
			UUID:       uuid.NewV4().Bytes(),
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

		//fmt.Printf("Generated message: %#v\n", message)

		wg.Add(1)
		go func() {
			err := es.Save([]domain.Event{
				message,
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
