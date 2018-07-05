package http

import (
	"github.com/roblaszczak/gooddd/message"
	"github.com/go-chi/chi"
	"net/http"
)

type UnmarshalMessageFunc func(topic string, request *http.Request) (message.Message, error)

type Subscriber struct {
	router chi.Router
	server *http.Server

	unmarshalMessageFunc UnmarshalMessageFunc
}

func NewSubscriber(addr string, unmarshalMessageFunc UnmarshalMessageFunc) (message.Subscriber, error) {
	r := chi.NewRouter()
	s := &http.Server{Addr: addr, Handler: r}

	return &Subscriber{r, s, unmarshalMessageFunc}, nil
}

func (s Subscriber) Subscribe(topic string, consumerGroup message.ConsumerGroup) (chan message.Message, error) {
	messages := make(chan message.Message)

	s.router.Post(topic, func(writer http.ResponseWriter, request *http.Request) {
		msg, err := s.unmarshalMessageFunc(topic, request)
		if err != nil {
			// todo - log
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		messages <- msg

		// todo - log
		select {
		case <-msg.Acknowledged():
		case <-request.Context().Done():
		}
	})

	// todo - how to handle errors?
	s.server.Close()
	go s.server.ListenAndServe()

	return messages, nil
}

func (s Subscriber) CloseSubscriber() error {
	return s.server.Close()
}
