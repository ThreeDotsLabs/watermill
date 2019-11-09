package post

import (
	"main.go/pkg/app/model"
)

type MemoryRepository struct {
	posts []model.Post
}

func (r *MemoryRepository) Create(post model.Post) error {
	r.posts = append(r.posts, post)
	return nil
}

func (r *MemoryRepository) Update(post model.Post) error {
	for i, p := range r.posts {
		if p.UUID == post.UUID {
			r.posts[i] = post
			break
		}
	}

	return nil
}
