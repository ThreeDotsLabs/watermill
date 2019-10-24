package post

import "main.go/pkg/app/model"

type MemoryRepository struct {
	posts []model.Post
}

func (r *MemoryRepository) Create(post model.Post) error {
	post.ID = len(r.posts) + 1
	r.posts = append(r.posts, post)

	return nil
}

func (r *MemoryRepository) Update(post model.Post) error {
	index := post.ID - 1
	r.posts[index] = post

	return nil
}
