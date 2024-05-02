package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"main/views"
	"net/http"
	"strconv"

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
	statsHandler := sseRouter.AddHandler(topic, &statsStream{storage: repo})

	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	e.GET("/", h.Index)
	e.POST("/posts/:id/reactions", h.AddReaction)
	e.GET("/posts/:id/stats", func(c echo.Context) error {
		postID := c.Param("id")
		c.Request().SetPathValue("id", postID)

		statsHandler(c.Response(), c.Request())
		return nil
	})

	return e
}

func (h Handler) Index(c echo.Context) error {
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

	return c.NoContent(http.StatusAccepted)
}

type statsStream struct {
	storage *Repository
}

func (s *statsStream) InitialStreamResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool) {
	postIDStr := r.PathValue("id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid post ID"))
		return nil, false
	}

	resp, err := s.getResponse(r.Context(), postID, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
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

	resp, err := s.getResponse(r.Context(), postID, &event)
	if err != nil {
		fmt.Println("could not get response: " + err.Error())
		return nil, false
	}

	return resp, true
}

func (s *statsStream) getResponse(ctx context.Context, postID int, event *PostStatsUpdated) (interface{}, error) {
	post, err := s.storage.PostByID(ctx, postID)
	if err != nil {
		return nil, err
	}

	var reactions []views.Reaction

	for _, r := range allReactions {
		reactions = append(reactions, views.Reaction{
			ID:          r.ID,
			Label:       r.Label,
			Count:       fmt.Sprint(post.Reactions[r.ID]),
			JustChanged: event != nil && event.ReactionUpdated != nil && *event.ReactionUpdated == r.ID,
		})
	}

	stats := views.PostStats{
		PostID: fmt.Sprint(post.ID),
		Views: views.PostViews{
			Count:       fmt.Sprint(post.Views),
			JustChanged: event != nil && event.ViewsUpdated,
		},
		Reactions: reactions,
	}

	var buffer bytes.Buffer
	err = views.PostStatsView(stats).Render(ctx, &buffer)
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
