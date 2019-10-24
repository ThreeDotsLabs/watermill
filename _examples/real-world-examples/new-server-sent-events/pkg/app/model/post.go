package model

type Post struct {
	ID      int
	Title   string
	Content string
	Author  string
}

type PostCreated struct {
	Post Post
}

type PostUpdated struct {
	Post Post
}
