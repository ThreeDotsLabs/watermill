package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/ascendsoftware/watermill"
	watermillHTTP "github.com/ascendsoftware/watermill-http/pkg/http"
	"github.com/ascendsoftware/watermill-routing-example/server/common"
	"github.com/ascendsoftware/watermill/message"
	"github.com/go-chi/chi/v5"
)

type Handler struct {
	storage *storage

	subscriber message.Subscriber
	publisher  message.Publisher
	logger     watermill.LoggerAdapter

	lastIDs map[string]int
}

func (h Handler) Mux() *chi.Mux {
	r := chi.NewRouter()

	fileServer(r, "/", http.Dir("./public"))

	sseRouter, err := watermillHTTP.NewSSERouter(
		watermillHTTP.SSERouterConfig{
			UpstreamSubscriber: h.subscriber,
		},
		h.logger,
	)
	if err != nil {
		panic(err)
	}

	messagesHandler := sseRouter.AddHandler(common.UpdatesTopic, messagesStream{
		logger:  h.logger,
		storage: h.storage,
	})

	r.Route("/api", func(r chi.Router) {
		r.Use(requestIDMiddleware)
		r.Get("/messages", messagesHandler)
		r.Post("/messages/{topic}", h.SendMessage)
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

func (h Handler) SendMessage(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")

	h.lastIDs[topic]++

	msgID := fmt.Sprintf("%v", h.lastIDs[topic])

	event := common.UserSignedUp{
		UserID: watermill.NewUUID(),
		Consents: common.Consents{
			Marketing: true,
			News:      true,
		},
	}

	payload, err := json.Marshal(event)
	if err != nil {
		logAndWriteError(h.logger, w, err)
		return
	}

	msg := message.NewMessage(msgID, payload)
	msg.Metadata.Set("name", "UserSignedUp")

	err = h.publisher.Publish(topic, msg)
	if err != nil {
		logAndWriteError(h.logger, w, err)
		return
	}

	w.WriteHeader(204)
}

type messagesStream struct {
	storage *storage
	logger  watermill.LoggerAdapter
}

func (p messagesStream) GetResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool) {
	reqID := r.Context().Value("request_id").(string)
	messages := p.storage.PopAll(reqID)
	return messages, true
}

func (p messagesStream) Validate(r *http.Request, msg *message.Message) (ok bool) {
	var payload common.MessageReceived
	err := json.Unmarshal(msg.Payload, &payload)
	if err != nil {
		return false
	}

	p.storage.Append(payload)

	return true
}

func fileServer(r chi.Router, path string, root http.FileSystem) {
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

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		requestID := watermill.NewUUID()
		ctx = context.WithValue(ctx, "request_id", requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
