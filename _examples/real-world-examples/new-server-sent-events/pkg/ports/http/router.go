package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi"
	"github.com/go-chi/render"

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

func FileServer(r chi.Router, path string, root http.FileSystem) {
	if strings.ContainsAny(path, "{}*") {
		panic("FileServer does not permit URL parameters.")
	}

	fs := http.StripPrefix(path, http.FileServer(root))

	if path != "/" && path[len(path)-1] != '/' {
		r.Get(path, http.RedirectHandler(path+"/", 301).ServeHTTP)
		path += "/"
	}
	path += "*"

	r.Get(path, func(w http.ResponseWriter, r *http.Request) {
		fs.ServeHTTP(w, r)
	})
}
