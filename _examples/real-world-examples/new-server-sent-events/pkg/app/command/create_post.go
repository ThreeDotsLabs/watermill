package command

import (
	"main.go/pkg/app/model"
)

type CreatePost struct {
	Title   string
	Content string
	Author  string
}

type createPostRepository interface {
	Create(post model.Post) error
}

type CreatePostHandler struct {
	repository createPostRepository
	publisher  eventPublisher
}

func NewCreatePostHandler(
	repository createPostRepository,
	publisher eventPublisher,
) CreatePostHandler {
	return CreatePostHandler{
		repository,
		publisher,
	}
}

func (h CreatePostHandler) Execute(command CreatePost) error {
	post := model.Post{
		Title:   command.Title,
		Content: command.Content,
		Author:  command.Author,
	}

	err := h.repository.Create(post)
	if err != nil {
		return err
	}

	event := model.PostCreated{
		Post: post,
	}

	err = h.publisher.Publish(event)
	if err != nil {
		return err
	}

	return nil
}
