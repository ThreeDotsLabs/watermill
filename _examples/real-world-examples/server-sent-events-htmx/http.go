package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"main/views"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	watermillhttp "github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type Handler struct {
	repo     *Repository
	eventBus *cqrs.EventBus
}

func NewHandler(repo *Repository, eventBus *cqrs.EventBus, sseRouter watermillhttp.SSERouter) *echo.Echo {
	h := Handler{
		repo:     repo,
		eventBus: eventBus,
	}

	marshaler := cqrs.JSONMarshaler{}
	topic := marshaler.Name(PostStatsUpdated{})
	statsHandler := sseRouter.AddHandler(topic, &statsStream{repo: repo})

	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	counter := sseHandlersCounter{}

	e.GET("/", h.AllPosts)
	e.POST("/posts/:id/reactions", h.AddReaction)
	e.GET("/posts/:id/stats", func(c echo.Context) error {
		postID := c.Param("id")
		c.Request().SetPathValue("id", postID)

		statsHandler(c.Response(), c.Request())
		return nil
	}, counter.Middleware)

	go func() {
		for {
			fmt.Println("SSE handlers count:", counter.Count.Load())
			time.Sleep(60 * time.Second)
		}
	}()

	return e
}

func (h Handler) AllPosts(c echo.Context) error {
	posts, err := h.repo.AllPosts(c.Request().Context())
	if err != nil {
		return err
	}

	for _, post := range posts {
		event := PostViewed{
			PostID: post.ID,
		}

		err = h.eventBus.Publish(c.Request().Context(), event)
		if err != nil {
			return err
		}
	}

	var postViews []views.Post
	for _, post := range posts {
		postViews = append(postViews, newPostView(post))
	}

	return views.Index(postViews).Render(c.Request().Context(), c.Response())
}

func (h Handler) AddReaction(c echo.Context) error {
	postID, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		return err
	}

	reactionID := c.FormValue("reaction_id")

	var found bool
	for _, r := range allReactions {
		if r.ID == reactionID {
			found = true
			break
		}
	}

	if !found {
		return c.String(http.StatusBadRequest, "invalid reaction ID")
	}

	event := PostReactionAdded{
		PostID:     postID,
		ReactionID: reactionID,
	}

	err = h.eventBus.Publish(c.Request().Context(), event)
	if err != nil {
		return err
	}

	reaction := mustReactionByID(reactionID)

	return views.UpdatedButton(reaction.Label).Render(c.Request().Context(), c.Response())
}

type statsStream struct {
	repo *Repository
}

func (s *statsStream) InitialStreamResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool) {
	postIDStr := r.PathValue("id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid post ID"))
		return nil, false
	}

	post, err := s.repo.PostByID(r.Context(), postID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("could not get post"))
		return nil, false
	}

	stats := PostStats{
		ID:        post.ID,
		Views:     post.Views,
		Reactions: post.Reactions,
	}

	resp, err := newPostStatsView(r.Context(), stats)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return nil, false
	}

	return resp, true
}

func (s *statsStream) NextStreamResponse(r *http.Request, msg *message.Message) (response interface{}, ok bool) {
	postIDStr := r.PathValue("id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		fmt.Println("invalid post ID")
		return nil, false
	}

	var event PostStatsUpdated
	err = json.Unmarshal(msg.Payload, &event)
	if err != nil {
		fmt.Println("cannot unmarshal: " + err.Error())
		return "", false
	}

	if event.PostID != postID {
		return "", false
	}

	stats := PostStats{
		ID:              event.PostID,
		Views:           event.Views,
		ViewsUpdated:    event.ViewsUpdated,
		Reactions:       event.Reactions,
		ReactionUpdated: event.ReactionUpdated,
	}

	resp, err := newPostStatsView(r.Context(), stats)
	if err != nil {
		fmt.Println("could not get response: " + err.Error())
		return nil, false
	}

	return resp, true
}

func newPostStatsView(ctx context.Context, stats PostStats) (interface{}, error) {
	var reactions []views.Reaction

	for _, r := range allReactions {
		reactions = append(reactions, views.Reaction{
			ID:          r.ID,
			Label:       r.Label,
			Count:       fmt.Sprint(stats.Reactions[r.ID]),
			JustChanged: stats.ReactionUpdated != nil && *stats.ReactionUpdated == r.ID,
		})
	}

	view := views.PostStats{
		PostID: fmt.Sprint(stats.ID),
		Views: views.PostViews{
			Count:       fmt.Sprint(stats.Views),
			JustChanged: stats.ViewsUpdated,
		},
		Reactions: reactions,
	}

	var buffer bytes.Buffer
	err := views.PostStatsView(view).Render(ctx, &buffer)
	if err != nil {
		return nil, err
	}

	return buffer.String(), nil
}

func newPostView(p Post) views.Post {
	return views.Post{
		ID:      fmt.Sprint(p.ID),
		Content: p.Content,
		Author:  p.Author,
		Date:    p.CreatedAt.Format("02 Jan 2006 15:04"),
	}
}

type sseHandlersCounter struct {
	Count atomic.Int64
}

func (s *sseHandlersCounter) Middleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		s.Count.Add(1)
		defer s.Count.Add(-1)
		return next(c)
	}
}
