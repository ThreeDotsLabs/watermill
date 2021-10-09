package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v5"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"

	"github.com/ThreeDotsLabs/watermill"
	watermillHTTP "github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
)

var generatedTags = []string{"watermill", "golang", "pubsub", "unicorn", "HelloWorld", "example", "ThreeDotsLabs"}

type Router struct {
	Subscriber   message.Subscriber
	Publisher    Publisher
	PostsStorage PostsStorage
	FeedsStorage FeedsStorage
	Logger       watermill.LoggerAdapter
}

func (router Router) Mux() *chi.Mux {
	r := chi.NewRouter()

	root := http.Dir("./public")
	FileServer(r, "/", root)

	sseRouter, err := watermillHTTP.NewSSERouter(
		watermillHTTP.SSERouterConfig{
			UpstreamSubscriber: router.Subscriber,
			ErrorHandler:       watermillHTTP.DefaultErrorHandler,
		},
		router.Logger,
	)
	if err != nil {
		panic(err)
	}

	postStream := postStreamAdapter{storage: router.PostsStorage, logger: router.Logger}
	feedStream := feedStreamAdapter{storage: router.FeedsStorage, logger: router.Logger}
	allFeedsStream := allFeedsStreamAdapter{storage: router.FeedsStorage, logger: router.Logger}

	postHandler := sseRouter.AddHandler(PostUpdatedTopic, postStream)
	feedHandler := sseRouter.AddHandler(FeedUpdatedTopic, feedStream)
	allFeedsHandler := sseRouter.AddHandler(FeedUpdatedTopic, allFeedsStream)

	r.Route("/api", func(r chi.Router) {
		r.Get("/posts/{id}", postHandler)
		r.Post("/posts", router.CreatePost)
		r.Post("/generate/post", router.GeneratePost)
		r.Patch("/posts/{id}", router.UpdatePost)
		r.Get("/feeds/{name}", feedHandler)
		r.Get("/feeds", allFeedsHandler)
	})

	go func() {
		err = sseRouter.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-sseRouter.Running()

	return r
}

type feedSummary struct {
	Name  string `json:"name"`
	Posts int    `json:"posts"`
}

type AllFeedsResponse struct {
	Feeds []feedSummary `json:"feeds"`
}

type allFeedsStreamAdapter struct {
	storage FeedsStorage
	logger  watermill.LoggerAdapter
}

func (f allFeedsStreamAdapter) GetResponse(w http.ResponseWriter, r *http.Request) (interface{}, bool) {
	feeds, err := f.storage.All(r.Context())
	if err != nil {
		logAndWriteError(f.logger, w, err)
		return nil, false
	}

	response := AllFeedsResponse{
		Feeds: []feedSummary{},
	}

	for _, f := range feeds {
		response.Feeds = append(response.Feeds, feedSummary{
			Name:  f.Name,
			Posts: len(f.Posts),
		})
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
		logAndWriteError(router.Logger, w, err)
		return
	}

	post := NewPost(
		watermill.NewUUID(),
		req.Title,
		req.Content,
		req.Author,
	)

	err = router.addPost(r.Context(), post)
	if err != nil {
		logAndWriteError(router.Logger, w, err)
		return
	}

	w.WriteHeader(204)
}

func (router Router) GeneratePost(w http.ResponseWriter, r *http.Request) {
	title := gofakeit.Sentence(5)
	content := gofakeit.Sentence(20)
	author := gofakeit.Name()

	tagsCount := rand.Intn(3) + 1
	for i := 0; i < tagsCount; i++ {
		content += fmt.Sprintf(" #%v", generatedTags[rand.Intn(len(generatedTags))])
	}

	post := NewPost(
		watermill.NewUUID(),
		title,
		content,
		author,
	)

	err := router.addPost(r.Context(), post)
	if err != nil {
		logAndWriteError(router.Logger, w, err)
		return
	}

	w.WriteHeader(204)
}

func (router Router) addPost(ctx context.Context, post Post) error {
	err := router.PostsStorage.Add(ctx, post)
	if err != nil {
		return err
	}

	event := PostCreated{
		Post:       post,
		OccurredAt: time.Now().UTC(),
	}

	err = router.Publisher.Publish(PostCreatedTopic, event)
	if err != nil {
		return err
	}

	return nil
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
		logAndWriteError(router.Logger, w, err)
		return
	}

	post, err := router.PostsStorage.ByID(r.Context(), req.ID)
	if err != nil {
		logAndWriteError(router.Logger, w, err)
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
		logAndWriteError(router.Logger, w, err)
		return
	}

	event := PostUpdated{
		OriginalPost: post,
		NewPost:      newPost,
		OccurredAt:   time.Now().UTC(),
	}

	err = router.Publisher.Publish(PostUpdatedTopic, event)
	if err != nil {
		logAndWriteError(router.Logger, w, err)
		return
	}

	w.WriteHeader(204)
}

type feedStreamAdapter struct {
	storage FeedsStorage
	logger  watermill.LoggerAdapter
}

func (f feedStreamAdapter) GetResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool) {
	feedName := chi.URLParam(r, "name")

	feed, err := f.storage.ByName(r.Context(), feedName)
	if err != nil {
		logAndWriteError(f.logger, w, err)
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
	logger  watermill.LoggerAdapter
}

func (p postStreamAdapter) GetResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool) {
	postID := chi.URLParam(r, "id")

	post, err := p.storage.ByID(r.Context(), postID)
	if err != nil {
		logAndWriteError(p.logger, w, err)
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

func logAndWriteError(logger watermill.LoggerAdapter, w http.ResponseWriter, err error) {
	logger.Error("Error", err, nil)
	w.WriteHeader(500)
}
