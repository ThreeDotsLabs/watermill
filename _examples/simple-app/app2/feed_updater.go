package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// intentionally not importing type from app1, because we don't need all data and we want to avoid coupling
type postAdded struct {
	OccurredOn time.Time `json:"occurred_on"`
	Author     string    `json:"author"`
	Title      string    `json:"title"`
}

type feedStorage interface {
	AddToFeed(title, author string, time time.Time) error
}

type printFeedStorage struct{}

func (printFeedStorage) AddToFeed(title, author string, time time.Time) error {
	fmt.Printf("Adding to feed: %s by %s @%s\n", title, author, time)
	return nil
}

type FeedGenerator struct {
	feedStorage feedStorage
}

func (f FeedGenerator) UpdateFeed(message *message.Message) ([]*message.Message, error) {
	event := postAdded{}
	json.Unmarshal(message.Payload, &event)

	err := f.feedStorage.AddToFeed(event.Title, event.Author, event.OccurredOn)
	if err != nil {
		return nil, errors.Wrap(err, "cannot update feed")
	}

	return nil, nil
}
