package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/render"
	"github.com/go-chi/chi"

	"github.com/ThreeDotsLabs/watermill"
	http2 "github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Router struct {
	MessageRouter *message.Router
	Subscriber    message.Subscriber
	Publisher     Publisher
	PostsStorage  PostsStorage
	FeedsStorage  FeedsStorage
	Logger        watermill.LoggerAdapter
}

func (router Router) Mux() *chi.Mux {
	r := chi.NewRouter()

	root := http.Dir("./public")
	FileServer(r, "/", root)

	sseRouter, err := http2.NewSSERouter(
		router.MessageRouter,
		router.Subscriber,
		http2.DefaultErrorHandler,
		router.Logger,
	)
	if err != nil {
		panic(err)
	}

	postStream := postStreamAdapter{storage: router.PostsStorage}
	feedStream := feedStreamAdapter{storage: router.FeedsStorage}
    allFeedsStream := allFeedsStreamAdapter{storage: router.FeedsStorage}

	postHandler := sseRouter.AddHandler(PostUpdatedTopic, postStream)
	feedHandler := sseRouter.AddHandler(FeedUpdatedTopic, feedStream)
    allFeedsHandler := sseRouter.AddHandler(FeedUpdatedTopic, allFeedsStream)

	r.Route("/api", func(r chi.Router) {
		r.Get("/posts/{id}", postHandler)
		r.Post("/posts", router.CreatePost)
		r.Patch("/posts/{id}", router.UpdatePost)
		r.Get("/feeds/{name}", feedHandler)
		r.Get("/feeds", allFeedsHandler)
	})

	return r
}

type AllFeedsResponse struct {
	Feeds []string `json:"feeds"`
}

type allFeedsStreamAdapter struct {
	storage FeedsStorage
}

func (f allFeedsStreamAdapter) GetResponse(w http.ResponseWriter, r *http.Request) (interface{}, bool) {
	names, err := f.storage.AllNames(r.Context())
	if err != nil {
		return nil, false
	}

	response := AllFeedsResponse{
		Feeds: names,
	}

    return response, true
}

func (f allFeedsStreamAdapter) Validate(r *http.Request, msg *message.Message) (ok bool) {
	return true
}

type CreatePostRequest struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Author  string `json:"author"`
}

func (router Router) CreatePost(w http.ResponseWriter, r *http.Request) {
	req := CreatePostRequest{}
	err := render.Decode(r, &req)
	if err != nil {
		router.logError(w, err)
		return
	}

	post := NewPost(
		watermill.NewUUID(),
		req.Title,
		req.Content,
		req.Author,
	)

	err = router.PostsStorage.Add(r.Context(), post)
	if err != nil {
		router.logError(w, err)
		return
	}

	event := PostCreated{
		Post:       post,
		OccurredAt: time.Now().UTC(),
	}

	err = router.Publisher.Publish(PostCreatedTopic, event)
	if err != nil {
		router.logError(w, err)
		return
	}

	w.WriteHeader(204)
}

type UpdatePostRequest struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

func (router Router) UpdatePost(w http.ResponseWriter, r *http.Request) {
	req := UpdatePostRequest{}
	err := render.Decode(r, &req)
	if err != nil {
		router.logError(w, err)
		return
	}

	post, err := router.PostsStorage.ByID(r.Context(), req.ID)
	if err != nil {
		router.logError(w, err)
		return
	}

	newPost := NewPost(
		post.ID,
		req.Title,
		req.Content,
		post.Author,
	)

	err = router.PostsStorage.Update(r.Context(), newPost)
	if err != nil {
		router.logError(w, err)
		return
	}

	event := PostUpdated{
		OriginalPost: post,
		NewPost:      newPost,
		OccurredAt:   time.Now().UTC(),
	}

	err = router.Publisher.Publish(PostUpdatedTopic, event)
	if err != nil {
		router.logError(w, err)
		return
	}

	w.WriteHeader(204)
}

func (router Router) logError(w http.ResponseWriter, err error) {
	router.Logger.Error("Error", err, nil)
	w.WriteHeader(500)
}

type feedStreamAdapter struct {
	storage FeedsStorage
}

func (f feedStreamAdapter) GetResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool) {
	feedName := chi.URLParam(r, "name")

	feed, err := f.storage.ByName(r.Context(), feedName)
	if err != nil {
		w.WriteHeader(500)
		return nil, false
	}

	return feed, true
}

func (f feedStreamAdapter) Validate(r *http.Request, msg *message.Message) (ok bool) {
	feedUpdated := FeedUpdated{}

	err := json.Unmarshal(msg.Payload, &feedUpdated)
	if err != nil {
		return false
	}

	feedName := chi.URLParam(r, "name")

	return feedUpdated.Name == feedName
}

type postStreamAdapter struct {
	storage PostsStorage
}

func (p postStreamAdapter) GetResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool) {
	postID := chi.URLParam(r, "id")

	post, err := p.storage.ByID(r.Context(), postID)
	if err != nil {
		w.WriteHeader(500)
		return nil, false
	}

	return post, true
}

func (p postStreamAdapter) Validate(r *http.Request, msg *message.Message) (ok bool) {
	postUpdated := PostUpdated{}

	err := json.Unmarshal(msg.Payload, &postUpdated)
	if err != nil {
		return false
	}

	postID := chi.URLParam(r, "id")

	return postUpdated.OriginalPost.ID == postID
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
