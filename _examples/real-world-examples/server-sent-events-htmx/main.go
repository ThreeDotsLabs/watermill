package main

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	stdHttp "net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var messages = map[string]string{
	"a": "🤩",
	"b": "⛄️",
	"c": "🌝",
	"d": "🙈",
}

var teams = map[string]Team{
	"a": {
		ID:    "a",
		Color: "danger",
	},
	"b": {
		ID:    "b",
		Color: "warning",
	},
	"c": {
		ID:    "c",
		Color: "primary",
	},
	"d": {
		ID:    "d",
		Color: "success",
	},
}

type Team struct {
	ID      string
	Color   string
	Count   int
	Percent int
}

//go:embed public/*.html
var staticFS embed.FS

//go:embed templates/*.html
var templatesFS embed.FS

func main() {
	logger := watermill.NewStdLogger(false, false)
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	sseRouter, err := http.NewSSERouter(http.SSERouterConfig{
		UpstreamSubscriber: pubSub,
		Marshaler:          http.BytesSSEMarshaler{},
	}, logger)

	if err != nil {
		panic(err)
	}

	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	e.StaticFS("/", echo.MustSubFS(staticFS, "public"))

	teamsHandler := sseRouter.AddHandler("teams-updated", teamsStreamAdapter{})
	messagesHandler := sseRouter.AddHandler("messages", messagesStreamAdapter{})

	e.GET("/api/teams", echo.WrapHandler(teamsHandler))
	e.GET("/api/messages", echo.WrapHandler(messagesHandler))

	e.POST("/api/join", func(c echo.Context) error {
		teamID := c.FormValue("team")

		team, ok := teams[teamID]
		if !ok {
			return c.String(stdHttp.StatusNotFound, "team not found")
		}

		team.Count++
		teams[teamID] = team

		var allCount int
		var keys []string
		for _, id := range []string{"a", "b", "c", "d"} {
			t := teams[id]
			if t.Count > 0 {
				allCount += t.Count
				keys = append(keys, t.ID)
			}
		}

		if len(keys) == 1 {
			t := teams[keys[0]]
			t.Percent = 100
			teams[keys[0]] = t
		} else if len(keys) > 1 {
			remainingPercent := 100
			for _, id := range keys[:len(keys)-1] {
				t := teams[id]
				t.Percent = int(float64(t.Count) / float64(allCount) * 100)
				remainingPercent -= t.Percent
				teams[id] = t
			}

			t := teams[keys[len(keys)-1]]
			t.Percent = remainingPercent
			teams[keys[len(keys)-1]] = t
		}

		msg := message.NewMessage(watermill.NewUUID(), []byte(nil))
		err := pubSub.Publish("teams-updated", msg)
		if err != nil {
			return c.String(stdHttp.StatusInternalServerError, err.Error())
		}

		return c.NoContent(stdHttp.StatusAccepted)
	})

	e.PUT("/api/messages/:message", func(c echo.Context) error {
		messageID := c.Param("message")

		_, ok := messages[messageID]
		if !ok {
			return c.String(stdHttp.StatusNotFound, "message not found")
		}

		msg := message.NewMessage(watermill.NewUUID(), []byte(messageID))
		err := pubSub.Publish("messages", msg)
		if err != nil {
			return c.String(stdHttp.StatusInternalServerError, err.Error())
		}

		return c.NoContent(stdHttp.StatusAccepted)
	})

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

type teamsStreamAdapter struct{}

func (t teamsStreamAdapter) InitialStreamResponse(w stdHttp.ResponseWriter, r *stdHttp.Request) (response interface{}, ok bool) {
	return t.getResponse()
}

func (t teamsStreamAdapter) NextStreamResponse(r *stdHttp.Request, msg *message.Message) (response interface{}, ok bool) {
	return t.getResponse()
}

func (t teamsStreamAdapter) getResponse() (string, bool) {
	tmpl := template.Must(template.ParseFS(templatesFS, "templates/teams.html"))

	var buffer bytes.Buffer
	err := tmpl.Execute(&buffer, teams)
	if err != nil {
		fmt.Println(err)
		return "", false
	}

	return buffer.String(), true
}

type messagesStreamAdapter struct{}

func (m messagesStreamAdapter) InitialStreamResponse(w stdHttp.ResponseWriter, r *stdHttp.Request) (response interface{}, ok bool) {
	return "", true
}

func (m messagesStreamAdapter) NextStreamResponse(r *stdHttp.Request, msg *message.Message) (response interface{}, ok bool) {
	text, ok := messages[string(msg.Payload)]
	if !ok {
		return "", false
	}
	return fmt.Sprintf("<p>%v</p>", text), true
}
