package http

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"

	"main.go/pkg/app/model"
	"main.go/pkg/app/query"
)

func (router Router) GetFeed(w http.ResponseWriter, r *http.Request) {
	feedID, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		w.WriteHeader(400)
		return
	}

	if render.GetAcceptedContentType(r) != render.ContentTypeEventStream {
		feed, ok := router.feedByID(w, feedID)
		if !ok {
			return
		}

		render.Respond(w, r, feed)
		return
	}

	responsesChannel := make(chan interface{})

	feed, ok := router.feedByID(w, feedID)
	if !ok {
		return
	}

	topic := router.TopicFunc(model.FeedUpdated{})
	messages, err := router.Subscriber.Subscribe(r.Context(), topic)
	if err != nil {
		w.WriteHeader(500)
		return
	}

	go func() {
		responsesChannel <- feed
		defer close(responsesChannel)

		for msg := range messages {
			feedUpdated := model.FeedUpdated{}
			err := json.Unmarshal(msg.Payload, &feedUpdated)
			if err != nil {
				panic(err)
			}

			if feedUpdated.ID != feedID {
				continue
			}

			feed, ok := router.feedByID(w, feedID)
			if !ok {
				return
			}

			select {
			case <-r.Context().Done():
				break
			default:
			}

			responsesChannel <- feed
			msg.Ack()
		}
	}()

	render.Respond(w, r, responsesChannel)
}

func (router Router) feedByID(w http.ResponseWriter, feedID int) (model.Feed, bool) {
	feed, err := router.FeedByIDHandler.Handle(query.FeedByID{feedID})
	if err != nil {
		w.WriteHeader(500)
		return model.Feed{}, false
	}

	return feed, true
}
