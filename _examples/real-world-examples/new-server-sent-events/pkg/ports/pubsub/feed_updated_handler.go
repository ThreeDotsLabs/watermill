package pubsub

import (
	"context"
	"errors"

	"main.go/pkg/app/model"
)

type FeedUpdatedHandler struct {
}

func (h FeedUpdatedHandler) HandlerName() string {
	return "FeedUpdatedHandler"
}

func (h FeedUpdatedHandler) NewEvent() interface{} {
	return &model.FeedUpdated{}
}

func (h FeedUpdatedHandler) Handle(ctx context.Context, event interface{}) error {
	e, ok := event.(model.FeedUpdated)
	if !ok {
		return errors.New("invalid event received")
	}

	_ = e

	return nil
}
