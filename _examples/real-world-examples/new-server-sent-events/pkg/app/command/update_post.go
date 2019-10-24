package command

import "main.go/pkg/app/model"

type UpdatePost struct {
	ID      int
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
		ID:      command.ID,
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
