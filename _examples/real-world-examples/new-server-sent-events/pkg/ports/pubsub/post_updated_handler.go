package pubsub

import (
	"context"
	"errors"

	"main.go/pkg/app/command"
	"main.go/pkg/app/model"
)

type PostUpdatedHandler struct {
	UpdatePostInFeedsHandler command.UpdatePostInFeedsHandler
}

func (h PostUpdatedHandler) HandlerName() string {
	return "PostUpdatedHandler"
}

func (h PostUpdatedHandler) NewEvent() interface{} {
	return &model.PostUpdated{}
}

func (h PostUpdatedHandler) Handle(ctx context.Context, event interface{}) error {
	e, ok := event.(model.PostUpdated)
	if !ok {
		return errors.New("invalid event received")
	}

	cmd := command.UpdatePostInFeeds{
		Post: e.Post,
	}

	err := h.UpdatePostInFeedsHandler.Execute(cmd)
	if err != nil {
		return err
	}

	return nil
}
