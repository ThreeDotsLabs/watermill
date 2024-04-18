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

var reactions = map[string]string{
	"star-struck": "🤩",
	"snowman":     "⛄️",
	"moon":        "🌝",
	"see-no-evil": "🙈",
	"thinking":    "🤔",
	"coffee":      "☕️",
	"t-rex":       "🦖",
	"heart":       "🩵",
}

type Team struct {
	ID      int
	Name    string
	Color   string
	Members int
	Percent int
}

type PlayerJoined struct {
	TeamID int `json:"team_id"`
}

type ReactionSent struct {
	ReactionID string `json:"reaction_id"`
	TeamID     int    `json:"team_id"`
}

type config struct {
	Port            int    `envconfig:"PORT" required:"true"`
	DatabaseURL     string `envconfig:"DATABASE_URL" required:"true"`
	PubSubProjectID string `envconfig:"PUBSUB_PROJECT_ID" required:"true"`
}

var migration = `
CREATE TABLE IF NOT EXISTS teams (
	id serial PRIMARY KEY,
	name VARCHAR NOT NULL,
	color VARCHAR NOT NULL,
	members INT NOT NULL,
	percent INT NOT NULL
);

INSERT INTO teams (id, name, color, members, percent) VALUES
	(1, 'Red', 'danger', 1, 25),
	(2, 'Yellow', 'warning', 1, 25),
	(3, 'Blue', 'primary', 1, 25),
	(4, 'Green', 'success', 1, 25)
ON CONFLICT (id) DO NOTHING
`

type User struct {
	TeamID int
}

type IndexData struct {
	User      *User
	Teams     []Team
	Reactions map[string]string
}

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

	storage := NewStorage(db)

	logger := watermill.NewStdLogger(false, false)

	subscriber, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		GenerateSubscriptionName: func(topic string) string {
			return topic
		},
		ProjectID: cfg.PubSubProjectID,
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

	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	e.GET("/", func(c echo.Context) error {
		teams, err := storage.AllTeams(context.Background())
		if err != nil {
			return err
		}

		data := IndexData{
			Teams:     teams,
			Reactions: reactions,
		}

		team, err := c.Cookie("team")
		if err == nil {
			teamID, err := strconv.Atoi(team.Value)
			if err != nil {
				return err
			}
			data.User = &User{
				TeamID: teamID,
			}
		}

		viewTeams := newViewTeams(teams)
		return views.Index(viewTeams, reactions).Render(c.Request().Context(), c.Response())
	})

	teamsHandler := sseRouter.AddHandler("teams-updated", teamsStreamAdapter{
		storage: storage,
	})
	publicChatHandler := sseRouter.AddHandler("reaction-sent", publicChatStreamAdapter{
		storage: storage,
	})

	e.GET("/api/teams", echo.WrapHandler(teamsHandler))
	e.GET("/api/public-chat", echo.WrapHandler(publicChatHandler))

	e.POST("/api/join", func(c echo.Context) error {
		teamIDStr := c.FormValue("team")
		teamID, err := strconv.Atoi(teamIDStr)
		if err != nil {
			return err
		}

		_, err = storage.TeamByID(c.Request().Context(), teamID)
		if err != nil {
			return err
		}

		event := PlayerJoined{
			TeamID: teamID,
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)
		err = publisher.Publish("player-joined", msg)
		if err != nil {
			return c.String(stdHttp.StatusInternalServerError, err.Error())
		}

		c.SetCookie(&stdHttp.Cookie{
			Name:     "team",
			Value:    teamIDStr,
			HttpOnly: true,
		})

		return c.NoContent(stdHttp.StatusAccepted)
	})

	e.PUT("/api/reactions/:reactionID", func(c echo.Context) error {
		reactionID := c.Param("reactionID")

		_, ok := reactions[reactionID]
		if !ok {
			return c.String(stdHttp.StatusNotFound, "reaction not found")
		}

		teamIDStr, err := c.Cookie("team")
		if err != nil {
			return err
		}
		teamID, err := strconv.Atoi(teamIDStr.Value)
		if err != nil {
			return err
		}

		_, err = storage.TeamByID(c.Request().Context(), teamID)
		if err != nil {
			return err
		}

		event := ReactionSent{
			ReactionID: reactionID,
			TeamID:     teamID,
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return c.String(stdHttp.StatusInternalServerError, err.Error())
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)
		err = publisher.Publish("reaction-sent", msg)
		if err != nil {
			return c.String(stdHttp.StatusInternalServerError, err.Error())
		}

		return c.NoContent(stdHttp.StatusAccepted)
	})

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(watermillmiddleware.Recoverer)

	router.AddHandler(
		"player-joined",
		"player-joined",
		subscriber,
		"teams-updated",
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			var event PlayerJoined
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return nil, err
			}

			err = storage.UpdateTeams(msg.Context(), func(teams []*Team) {
				var allCount int

				for _, t := range teams {
					if t.ID == event.TeamID {
						t.Members++
					}

					allCount += t.Members
				}

				remainingPercent := 100
				for _, t := range teams[:len(teams)-1] {
					t.Percent = int(float64(t.Members) / float64(allCount) * 100)
					remainingPercent -= t.Percent
				}

				lastTeam := teams[len(teams)-1]
				lastTeam.Percent = remainingPercent
			})
			if err != nil {
				return nil, err
			}

			newMsg := message.NewMessage(watermill.NewUUID(), []byte(nil))
			return []*message.Message{newMsg}, nil
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

	err = e.Start(":8080")
	if err != nil {
		panic(err)
	}
}

type teamsStreamAdapter struct {
	storage *Storage
}

func (t teamsStreamAdapter) InitialStreamResponse(w stdHttp.ResponseWriter, r *stdHttp.Request) (response interface{}, ok bool) {
	return t.getResponse()
}

func (t teamsStreamAdapter) NextStreamResponse(r *stdHttp.Request, msg *message.Message) (response interface{}, ok bool) {
	return t.getResponse()
}

func (t teamsStreamAdapter) getResponse() (string, bool) {
	teams, err := t.storage.AllTeams(context.Background())
	if err != nil {
		fmt.Println(err)
		return "", false
	}

	viewTeams := newViewTeams(teams)

	var buf bytes.Buffer
	err = views.TeamsBar(viewTeams).Render(context.Background(), &buf)
	if err != nil {
		fmt.Println(err)
		return "", false
	}
	return buf.String(), true
}

type publicChatStreamAdapter struct {
	storage *Storage
}

func (m publicChatStreamAdapter) InitialStreamResponse(w stdHttp.ResponseWriter, r *stdHttp.Request) (response interface{}, ok bool) {
	return "", true
}

func (m publicChatStreamAdapter) NextStreamResponse(r *stdHttp.Request, msg *message.Message) (response interface{}, ok bool) {
	var event ReactionSent
	err := json.Unmarshal(msg.Payload, &event)
	if err != nil {
		fmt.Println("cannot unmarshal: " + err.Error())
		return "", false
	}

	text, ok := reactions[event.ReactionID]
	if !ok {
		fmt.Println("unknown reaction:" + event.ReactionID)
		return "", false
	}

	team, err := m.storage.TeamByID(context.Background(), event.TeamID)
	if err != nil {
		fmt.Println("could not get team: " + err.Error())
		return "", false
	}

	return fmt.Sprintf(`<span class="circle bg-%v">%v</span>`, team.Color, text), true
}

type Storage struct {
	db *sql.DB
}

func NewStorage(db *sql.DB) *Storage {
	return &Storage{
		db: db,
	}
}

func (s *Storage) TeamByID(ctx context.Context, id int) (Team, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, name, color, members, percent FROM teams WHERE id = $1`, id)
	team, err := scanTeam(row)
	if err != nil {
		return Team{}, err
	}

	return team, nil
}

func (s *Storage) AllTeams(ctx context.Context) ([]Team, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, color, members, percent FROM teams ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}

	var teams []Team
	for rows.Next() {
		team, err := scanTeam(rows)
		if err != nil {
			return nil, err
		}
		teams = append(teams, team)
	}

	return teams, nil
}

func (s *Storage) UpdateTeams(ctx context.Context, updateFn func(teams []*Team)) (err error) {
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

	rows, err := tx.QueryContext(ctx, `SELECT id, name, color, members, percent FROM teams FOR UPDATE`)
	if err != nil {
		return err
	}

	var teams []*Team
	for rows.Next() {
		team, err := scanTeam(rows)
		if err != nil {
			return err
		}
		teams = append(teams, &team)
	}

	updateFn(teams)

	for _, t := range teams {
		_, err := tx.ExecContext(ctx, `UPDATE teams SET members = $1, percent = $2 WHERE id = $3`, t.Members, t.Percent, t.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTeam(s scanner) (Team, error) {
	var name, color string
	var id, members, percent int
	err := s.Scan(&id, &name, &color, &members, &percent)
	if err != nil {
		return Team{}, err
	}

	return Team{
		ID:      id,
		Name:    name,
		Color:   color,
		Members: members,
		Percent: percent,
	}, nil
}

func newViewTeams(teams []Team) []views.Team {
	var viewTeams []views.Team
	for _, t := range teams {
		viewTeams = append(viewTeams, views.Team{
			ID:      strconv.Itoa(t.ID),
			Name:    t.Name,
			Color:   t.Color,
			Members: strconv.Itoa(t.Members),
			Percent: strconv.Itoa(t.Percent),
		})
	}
	return viewTeams
}
