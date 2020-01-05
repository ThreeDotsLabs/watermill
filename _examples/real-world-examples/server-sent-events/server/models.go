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
	Tags    []string `json:"tags" bson:"tags"`
}

func NewPost(id, title, content, author string) Post {
	pattern := regexp.MustCompile("#([a-zA-Z0-9]+)")
	matches := pattern.FindAllStringSubmatch(content, -1)

	var tags []string
	tagsMap := map[string]struct{}{}

	for _, tag := range matches {
		tagSlug := strings.ToLower(tag[1])

		_, ok := tagsMap[tagSlug]
		if ok {
			continue
		}

		tagsMap[tagSlug] = struct{}{}
		tags = append(tags, tagSlug)
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
