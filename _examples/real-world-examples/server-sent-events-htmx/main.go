package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"main/views"
	stdHttp "net/http"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	watermillmiddleware "github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/kelseyhightower/envconfig"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/lib/pq"
)

const (
	postViewedTopic        = "post-viewed"
	postReactionAddedTopic = "post-reaction-added"
	postStatsUpdatedTopic  = "post-stats-updated"
)

var allReactions = []Reaction{
	{
		ID:    "fire",
		Label: "🔥",
	},
	{
		ID:    "thinking",
		Label: "🤔",
	},
	{
		ID:    "heart",
		Label: "🩵",
	},
	{
		ID:    "laugh",
		Label: "😂",
	},
	{
		ID:    "sad",
		Label: "😢",
	},
}

type Reaction struct {
	ID    string
	Label string
}

type Post struct {
	ID        int
	Author    string
	Content   string
	CreatedAt time.Time
	Views     int
	Reactions map[string]int
}

type PostViewed struct {
	PostID int `json:"post_id"`
}

type PostReactionAdded struct {
	PostID     int    `json:"post_id"`
	ReactionID string `json:"reaction_id"`
}

type PostStatsUpdated struct {
	PostID int `json:"post_id"`
}

type config struct {
	Port            int    `envconfig:"PORT" required:"true"`
	DatabaseURL     string `envconfig:"DATABASE_URL" required:"true"`
	PubSubProjectID string `envconfig:"PUBSUB_PROJECT_ID" required:"true"`
}

var migration = `
CREATE TABLE IF NOT EXISTS posts (
	id serial PRIMARY KEY,
	author VARCHAR NOT NULL,
	content TEXT NOT NULL,
	views INT NOT NULL DEFAULT 0,
    reactions JSONB NOT NULL DEFAULT '{}',
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO posts (id, author, content) VALUES
	(1, 'Miłosz', 'Oh, I remember the days when we used to write code in PHP!'),
	(2, 'Robert', 'Back in my days, we used to write code in assembly!'),
	(3, 'Miłosz', 'Go is my favorite language!')
ON CONFLICT (id) DO NOTHING;
`

func main() {
	var cfg config
	err := envconfig.Process("", &cfg)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(migration)
	if err != nil {
		panic(err)
	}

	logger := watermill.NewStdLogger(false, false)

	subscriber, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: cfg.PubSubProjectID,
		GenerateSubscriptionName: func(topic string) string {
			return fmt.Sprintf("%v_%v", topic, watermill.NewShortUUID())
		},
		SubscriptionConfig: pubsub.SubscriptionConfig{
			ExpirationPolicy: time.Hour * 24,
		},
	}, logger)
	if err != nil {
		panic(err)
	}

	publisher, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID: cfg.PubSubProjectID,
	}, logger)
	if err != nil {
		panic(err)
	}

	sseRouter, err := http.NewSSERouter(http.SSERouterConfig{
		UpstreamSubscriber: subscriber,
		Marshaler:          http.BytesSSEMarshaler{},
	}, logger)
	if err != nil {
		panic(err)
	}

	storage := NewStorage(db)

	statsHandler := sseRouter.AddHandler(postStatsUpdatedTopic, statsStream{storage: storage})

	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	e.GET("/", func(c echo.Context) error {
		posts, err := storage.AllPosts(c.Request().Context())
		if err != nil {
			return err
		}

		for _, post := range posts {
			event := PostViewed{
				PostID: post.ID,
			}

			payload, err := json.Marshal(event)
			if err != nil {
				return err
			}

			msg := message.NewMessage(watermill.NewUUID(), payload)
			err = publisher.Publish(postViewedTopic, msg)
			if err != nil {
				return err
			}
		}

		var postViews []views.Post
		for _, post := range posts {
			postViews = append(postViews, newPostView(post))
		}

		return views.Index(postViews).Render(c.Request().Context(), c.Response())
	})

	e.POST("/posts/:id/reactions", func(c echo.Context) error {
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
			return c.String(stdHttp.StatusBadRequest, "invalid reaction ID")
		}

		event := PostReactionAdded{
			PostID:     postID,
			ReactionID: reactionID,
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)
		err = publisher.Publish(postReactionAddedTopic, msg)
		if err != nil {
			return err
		}

		return c.NoContent(stdHttp.StatusAccepted)
	})

	e.GET("/posts/:id/stats", func(c echo.Context) error {
		postID := c.Param("id")
		c.Request().SetPathValue("id", postID)

		statsHandler(c.Response(), c.Request())
		return nil
	})

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(watermillmiddleware.Recoverer)

	router.AddHandler(
		"on-post-viewed",
		postViewedTopic,
		subscriber,
		postStatsUpdatedTopic,
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			var event PostViewed
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return nil, err
			}

			err = storage.UpdatePost(context.Background(), event.PostID, func(post *Post) {
				post.Views++
			})
			if err != nil {
				return nil, err
			}

			newEvent := PostStatsUpdated{
				PostID: event.PostID,
			}

			payload, err := json.Marshal(newEvent)
			if err != nil {
				return nil, err
			}

			return []*message.Message{
				message.NewMessage(watermill.NewUUID(), payload),
			}, nil
		},
	)

	router.AddHandler(
		"on-post-reaction-added",
		postReactionAddedTopic,
		subscriber,
		postStatsUpdatedTopic,
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			var event PostReactionAdded
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return nil, err
			}

			err = storage.UpdatePost(context.Background(), event.PostID, func(post *Post) {
				post.Reactions[event.ReactionID]++
			})
			if err != nil {
				return nil, err
			}

			newEvent := PostStatsUpdated{
				PostID: event.PostID,
			}

			payload, err := json.Marshal(newEvent)
			if err != nil {
				return nil, err
			}

			return []*message.Message{
				message.NewMessage(watermill.NewUUID(), payload),
			}, nil
		},
	)

	go func() {
		err := router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		err := sseRouter.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	err = e.Start(fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		panic(err)
	}
}

type statsStream struct {
	storage *Storage
}

func (s statsStream) InitialStreamResponse(w stdHttp.ResponseWriter, r *stdHttp.Request) (response interface{}, ok bool) {
	postIDStr := r.PathValue("id")
	postID, err := strconv.Atoi(postIDStr)
	if err != nil {
		w.WriteHeader(stdHttp.StatusBadRequest)
		_, _ = w.Write([]byte("invalid post ID"))
		return nil, false
	}

	resp, err := s.getResponse(postID)
	if err != nil {
		w.WriteHeader(stdHttp.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return nil, false
	}

	return resp, true
}

func (s statsStream) NextStreamResponse(r *stdHttp.Request, msg *message.Message) (response interface{}, ok bool) {
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

	resp, err := s.getResponse(postID)
	if err != nil {
		fmt.Println("could not get response: " + err.Error())
		return nil, false
	}

	return resp, true
}

func (s statsStream) getResponse(postID int) (interface{}, error) {
	post, err := s.storage.PostByID(context.Background(), postID)
	if err != nil {
		return nil, err
	}

	var reactions []views.Reaction

	for _, r := range allReactions {
		reactions = append(reactions, views.Reaction{
			ID:    r.ID,
			Label: r.Label,
			Count: fmt.Sprint(post.Reactions[r.ID]),
		})
	}

	stats := views.PostStats{
		PostID:    fmt.Sprint(post.ID),
		Views:     fmt.Sprint(post.Views),
		Reactions: reactions,
	}

	var buffer bytes.Buffer
	err = views.PostStatsView(stats).Render(context.Background(), &buffer)
	if err != nil {
		return nil, err
	}

	return buffer.String(), nil
}

type Storage struct {
	db *sql.DB
}

func NewStorage(db *sql.DB) *Storage {
	return &Storage{
		db: db,
	}
}

func (s *Storage) PostByID(ctx context.Context, id int) (Post, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, author, content, views, reactions, created_at FROM posts WHERE id = $1`, id)
	post, err := scanPost(row)
	if err != nil {
		return Post{}, err
	}

	return post, nil
}

func (s *Storage) AllPosts(ctx context.Context) ([]Post, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, author, content, views, reactions, created_at FROM posts ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}

	var posts []Post
	for rows.Next() {
		post, err := scanPost(rows)
		if err != nil {
			return nil, err
		}
		posts = append(posts, post)
	}

	return posts, nil
}

func (s *Storage) UpdatePost(ctx context.Context, id int, updateFn func(post *Post)) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			txErr := tx.Rollback()
			if txErr != nil {
				err = txErr
			}
		}
	}()

	row := s.db.QueryRowContext(ctx, `SELECT id, author, content, views, reactions, created_at FROM posts WHERE id = $1 FOR UPDATE`, id)
	post, err := scanPost(row)
	if err != nil {
		return err
	}

	updateFn(&post)

	reactionsJSON, err := json.Marshal(post.Reactions)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `UPDATE posts SET views = $1, reactions = $2 WHERE id = $3`, post.Views, reactionsJSON, post.ID)
	if err != nil {
		return err
	}

	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanPost(s scanner) (Post, error) {
	var id, postViews int
	var author, content string
	var reactions []byte
	var createdAt time.Time

	err := s.Scan(&id, &author, &content, &postViews, &reactions, &createdAt)
	if err != nil {
		return Post{}, err
	}

	var reactionsMap map[string]int
	err = json.Unmarshal(reactions, &reactionsMap)
	if err != nil {
		return Post{}, err
	}

	return Post{
		ID:        id,
		Author:    author,
		Content:   content,
		CreatedAt: createdAt,
		Views:     postViews,
		Reactions: reactionsMap,
	}, nil
}

func newPostView(p Post) views.Post {
	return views.Post{
		ID:      fmt.Sprint(p.ID),
		Content: p.Content,
		Author:  p.Author,
		Date:    p.CreatedAt.Format("02 Jan 2006 15:04"),
	}
}
