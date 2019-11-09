package http

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"

	"main.go/pkg/app/model"
	"main.go/pkg/app/query"
)

type Router struct {
	FeedUpdatedChannel <-chan model.FeedUpdated
	FeedByIDHandler    query.FeedByIDHandler
}

func (router Router) Mux() *chi.Mux {
	r := chi.NewRouter()

	r.Post("/posts", CreatePost)
	r.Patch("/posts/{id}", UpdatePost)
	r.Get("/feeds/{id}", router.GetFeed)

	return r
}

func CreatePost(w http.ResponseWriter, r *http.Request) {

}

func UpdatePost(w http.ResponseWriter, r *http.Request) {

}

func (router Router) GetFeed(w http.ResponseWriter, r *http.Request) {
	feedID, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		// TODO handle
		panic(err)
	}

	if render.GetAcceptedContentType(r) != render.ContentTypeEventStream {
		response, err := router.FeedByIDHandler.Handle(query.FeedByID{feedID})
		if err != nil {
			// TODO handle
			panic(err)
		}

		render.Respond(w, r, response)
		return
	}

	responsesChannel := make(chan interface{}, 1)
	defer close(responsesChannel)

	response, err := router.FeedByIDHandler.Handle(query.FeedByID{feedID})
	if err != nil {
		// TODO handle
		panic(err)
	}

	responsesChannel <- response

	go func() {
		for e := range router.FeedUpdatedChannel {
			if e.ID != feedID {
				continue
			}

			response, err := router.FeedByIDHandler.Handle(query.FeedByID{feedID})
			if err != nil {
				// TODO handle
				panic(err)
			}

			responsesChannel <- response
		}
	}()

	render.Respond(w, r, responsesChannel)
}
