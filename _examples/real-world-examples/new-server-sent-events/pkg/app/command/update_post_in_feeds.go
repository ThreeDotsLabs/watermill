package command

import (
	"main.go/pkg/app/model"
)

type UpdatePostInFeeds struct {
	Post model.Post
}

type UpdatePostInFeedsHandler struct {
	repository feedRepository
	publisher  eventPublisher
}

func NewUpdatePostInFeedsHandler(
	feedRepository feedRepository,
	publisher eventPublisher,
) UpdatePostInFeedsHandler {
	return UpdatePostInFeedsHandler{
		feedRepository,
		publisher,
	}
}

func (h UpdatePostInFeedsHandler) Execute(command UpdatePostInFeeds) error {
	feeds, err := h.repository.All()
	if err != nil {
		return err
	}

	for _, feed := range feeds {
		err = h.repository.UpdatePostInFeed(feed, command.Post)
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
