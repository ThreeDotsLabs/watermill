package model

type Feed struct {
	ID    int
	Posts []Post
}

type FeedUpdated struct {
	ID int
}
