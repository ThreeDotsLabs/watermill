package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	stdHttp "net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

//go:embed public/*.html
var staticFS embed.FS

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

	go func() {
		count := 0
		for {
			time.Sleep(5 * time.Second)
			count++
			if count > 99 {
				count = 0
			}

			payload := notifications{
				Count: count,
			}
			bytes, err := json.Marshal(payload)
			if err != nil {
				panic(err)
			}

			msg := message.NewMessage(watermill.NewUUID(), bytes)
			err = pubSub.Publish("notifications", msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		var messages []string
		for {
			time.Sleep(5 * time.Second)
			messages = append(messages, "Hello, world!")
			if len(messages) > 5 {
				messages = nil
			}

			row := `
<div class="toast show" role="alert" aria-live="assertive" aria-atomic="true">
  <div class="toast-header">
    <strong class="me-auto">Bootstrap</strong>
    <small>11 mins ago</small>
  </div>
  <div class="toast-body">%s</div>
</div>
`

			var payload string

			for _, message := range messages {
				payload += fmt.Sprintf(row, message)
			}

			msg := message.NewMessage(watermill.NewUUID(), []byte(payload))
			err := pubSub.Publish("messages", msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	e.StaticFS("/", echo.MustSubFS(staticFS, "public"))

	notificationsHandler := sseRouter.AddHandler("notifications", notificationsStreamAdapter{})
	messagesHandler := sseRouter.AddHandler("messages", messagesStreamAdapter{})

	e.GET("/api/notifications", echo.WrapHandler(notificationsHandler))
	e.GET("/api/messages", echo.WrapHandler(messagesHandler))

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

type notifications struct {
	Count int `json:"count"`
}

type notificationsStreamAdapter struct{}

func (n notificationsStreamAdapter) InitialStreamResponse(w stdHttp.ResponseWriter, r *stdHttp.Request) (response interface{}, ok bool) {
	return 0, true
}

func (n notificationsStreamAdapter) NextStreamResponse(r *stdHttp.Request, msg *message.Message) (response interface{}, ok bool) {
	var payload notifications
	err := json.Unmarshal(msg.Payload, &payload)
	if err != nil {
		return nil, false
	}

	return payload.Count, true
}

type messagesStreamAdapter struct{}

func (m messagesStreamAdapter) InitialStreamResponse(w stdHttp.ResponseWriter, r *stdHttp.Request) (response interface{}, ok bool) {
	return "", true
}

func (m messagesStreamAdapter) NextStreamResponse(r *stdHttp.Request, msg *message.Message) (response interface{}, ok bool) {
	return string(msg.Payload), true
}
