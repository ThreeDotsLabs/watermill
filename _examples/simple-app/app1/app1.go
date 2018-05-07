package main

import (
	"github.com/satori/go.uuid"
	"github.com/Pallinder/go-randomdata"
	"github.com/roblaszczak/gooddd/domain/eventstore"
	"github.com/roblaszczak/gooddd/domain"
	"sync"
)

type postAdded struct {
	UUID string `json:"uuid"`

	Author string `json:"author"`
	Title  string `json:"title"`

	Tags []string `json:"tags"`

	Content string `json:"content"`
}

func (p postAdded) AggregateID() string {
	return p.UUID
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

	// todo - move it out1!
	eventsFactory := domain.NewEventsFactory(func() string {
		return uuid.NewV4().String()
	})

	i := 100000
	wg := &sync.WaitGroup{}

	for {
		event := postAdded{
			UUID: uuid.NewV4().String(),

			Author: randomdata.FullName(randomdata.RandomGender),
			Title:  randomdata.SillyName(),

			Tags: []string{
				randomdata.Adjective(),
				randomdata.Adjective(),
				randomdata.Noun(),
			},

			Content: randomdata.Paragraph(),
		}

		//fmt.Printf("Generated event: %#v\n", event)

		wg.Add(1)
		go func() {
			err := es.Save(eventsFactory.NewEvents([]domain.EventPayload{
				event,
			}))
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
