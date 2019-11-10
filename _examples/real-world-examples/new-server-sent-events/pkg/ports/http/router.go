package http

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi"

	"main.go/pkg/app/model"
	"main.go/pkg/app/query"
)

type Router struct {
	Subscriber      message.Subscriber
	TopicFunc       func(event interface{}) string
	FeedByIDHandler query.FeedByIDHandler
}

func (router Router) Mux() *chi.Mux {
	r := chi.NewRouter()

	root := http.Dir("./public")
	FileServer(r, "/", root)

	feedSSE := NewSSE(
		router.Subscriber,
		router.TopicFunc(model.FeedUpdated{}),
		func(w http.ResponseWriter, r *http.Request) (interface{}, bool) {
			feedID, err := strconv.Atoi(chi.URLParam(r, "id"))
			if err != nil {
				w.WriteHeader(400)
				return nil, false
			}

			feed, err := router.FeedByIDHandler.Handle(query.FeedByID{feedID})
			if err != nil {
				w.WriteHeader(500)
				return nil, false
			}

			return feed, true
		},
		func(r *http.Request, msg *message.Message) bool {
			feedUpdated := model.FeedUpdated{}

			err := json.Unmarshal(msg.Payload, &feedUpdated)
			if err != nil {
				return false
			}

			feedID, err := strconv.Atoi(chi.URLParam(r, "id"))
			if err != nil {
				return false
			}

			return feedUpdated.ID == feedID
		},
	)

	r.Post("/posts", CreatePost)
	r.Patch("/posts/{id}", UpdatePost)
	r.Get("/feeds/{id}", feedSSE.Handler)

	return r
}

func CreatePost(w http.ResponseWriter, r *http.Request) {

}

func UpdatePost(w http.ResponseWriter, r *http.Request) {

}
