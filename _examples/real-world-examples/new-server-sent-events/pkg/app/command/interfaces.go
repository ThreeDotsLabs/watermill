package command

import "main.go/pkg/app/model"

type eventPublisher interface {
	Publish(event interface{}) error
}

type feedRepository interface {
	All() ([]model.Feed, error)
	AddPostToFeed(feed model.Feed, post model.Post) error
	UpdatePostInFeed(feed model.Feed, post model.Post) error
}
