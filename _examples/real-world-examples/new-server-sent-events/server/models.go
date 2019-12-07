package main

import (
	"regexp"
	"strings"
	"time"
)

type Post struct {
	ID      string   `json:"id" bson:"id"`
	Title   string   `json:"title" bson:"title"`
	Content string   `json:"content" bson:"content"`
	Author  string   `json:"author" bson:"author"`
	Tags    []string `json:"-" bson:"-"`
}

func NewPost(id, title, content, author string) Post {
	pattern := regexp.MustCompile("#([a-zA-Z0-9]+)")
	matches := pattern.FindAllStringSubmatch(content, -1)

	var tags []string
	// Could use deduplication here
	for _, tag := range matches {
		tags = append(tags, strings.ToLower(tag[1]))
	}

	return Post{
		ID:      id,
		Title:   title,
		Content: content,
		Author:  author,
		Tags:    tags,
	}
}

type Feed struct {
	Name  string `json:"name" bson:"_id"`
	Posts []Post `json:"posts" bson:"posts"`
}

type PostCreated struct {
	Post Post `json:"post"`

	OccurredAt time.Time `json:"occurred_at"`
}

type PostUpdated struct {
	OriginalPost Post `json:"original_post"`
	NewPost      Post `json:"new_post"`

	OccurredAt time.Time `json:"occurred_at"`
}

type FeedUpdated struct {
	Name string `json:"name"`

	OccurredAt time.Time `json:"occurred_at"`
}
