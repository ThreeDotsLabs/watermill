package model

type Post struct {
	UUID    string
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
