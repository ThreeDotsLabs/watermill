package pubsub

import (
	"context"
	"errors"

	"main.go/pkg/app/command"
	"main.go/pkg/app/model"
)

type PostCreatedHandler struct {
	AddPostToFeedHandler command.AddPostToFeedsHandler
}

func (h PostCreatedHandler) HandlerName() string {
	return "PostCreatedHandler"
}

func (h PostCreatedHandler) NewEvent() interface{} {
	return &model.PostCreated{}
}

func (h PostCreatedHandler) Handle(ctx context.Context, event interface{}) error {
	e, ok := event.(model.PostCreated)
	if !ok {
		return errors.New("invalid event received")
	}

	cmd := command.AddPostToFeeds{
		Post: e.Post,
	}

	err := h.AddPostToFeedHandler.Execute(cmd)
	if err != nil {
		return err
	}

	return nil
}
