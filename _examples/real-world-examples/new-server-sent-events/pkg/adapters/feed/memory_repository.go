package feed

import (
	"main.go/pkg/app/model"
)

type MemoryRepository struct {
	feeds []model.Feed
}

func (r *MemoryRepository) All() ([]model.Feed, error) {
	return r.feeds, nil
}

func (r *MemoryRepository) ByID(feedID int) (model.Feed, error) {
	index := feedID - 1

	feed := r.feeds[index]

	return feed, nil
}

func (r *MemoryRepository) Add(feed model.Feed) error {
	feed.ID = len(r.feeds) + 1
	r.feeds = append(r.feeds, feed)

	return nil
}

func (r *MemoryRepository) AddPostToFeed(feed model.Feed, post model.Post) error {
	index := feed.ID - 1
	feed = r.feeds[index]
	feed.Posts = append([]model.Post{post}, feed.Posts...)
	r.feeds[index] = feed

	return nil
}

func (r *MemoryRepository) UpdatePostInFeed(feed model.Feed, post model.Post) error {
	index := feed.ID - 1
	feed = r.feeds[index]

	for i, p := range feed.Posts {
		if p.UUID == post.UUID {
			feed.Posts[i] = post
			break
		}
	}

	return nil
}
