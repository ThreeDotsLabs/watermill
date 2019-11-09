package http

import (
	"net/http"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi"

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

	r.Post("/posts", CreatePost)
	r.Patch("/posts/{id}", UpdatePost)
	r.Get("/feeds/{id}", router.GetFeed)

	return r
}

func CreatePost(w http.ResponseWriter, r *http.Request) {

}

func UpdatePost(w http.ResponseWriter, r *http.Request) {

}
