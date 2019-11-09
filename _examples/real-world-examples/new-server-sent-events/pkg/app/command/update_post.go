package command

import "main.go/pkg/app/model"

type UpdatePost struct {
	UUID    string
	Title   string
	Content string
	Author  string
}

type updatePostRepository interface {
	Update(post model.Post) error
}

type UpdatePostHandler struct {
	repository updatePostRepository
	publisher  eventPublisher
}

func NewUpdatePostHandler(
	repository updatePostRepository,
	publisher eventPublisher,
) UpdatePostHandler {
	return UpdatePostHandler{
		repository,
		publisher,
	}
}

func (h UpdatePostHandler) Execute(command UpdatePost) error {
	post := model.Post{
		UUID:    command.UUID,
		Title:   command.Title,
		Content: command.Content,
		Author:  command.Author,
	}

	err := h.repository.Update(post)
	if err != nil {
		return err
	}

	event := model.PostUpdated{
		Post: post,
	}

	err = h.publisher.Publish(event)
	if err != nil {
		return err
	}

	return nil
}
