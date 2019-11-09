package query

import "main.go/pkg/app/model"

type FeedByID struct {
	FeedID int
}

type feedByIDFinder interface {
	ByID(feedID int) (model.Feed, error)
}

type FeedByIDHandler struct {
	FeedByIDFinder feedByIDFinder
}

func (h FeedByIDHandler) Handle(query FeedByID) (model.Feed, error) {
	return h.FeedByIDFinder.ByID(query.FeedID)
}
