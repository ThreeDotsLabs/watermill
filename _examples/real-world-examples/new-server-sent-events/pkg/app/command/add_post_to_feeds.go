package command

import (
	"main.go/pkg/app/model"
)

type AddPostToFeeds struct {
	Post model.Post
}

type AddPostToFeedsHandler struct {
	repository feedRepository
	publisher  eventPublisher
}

func NewAddPostToFeedsHandler(
	feedRepository feedRepository,
	publisher eventPublisher,
) AddPostToFeedsHandler {
	return AddPostToFeedsHandler{
		feedRepository,
		publisher,
	}
}

func (h AddPostToFeedsHandler) Execute(command AddPostToFeeds) error {
	feeds, err := h.repository.All()
	if err != nil {
		return err
	}

	for _, feed := range feeds {
		err = h.repository.AddPostToFeed(feed, command.Post)
		if err != nil {
			return err
		}

		event := model.FeedUpdated{
			ID: feed.ID,
		}

		err = h.publisher.Publish(event)
		if err != nil {
			return err
		}
	}

	return nil
}
