package http

import (
	"net/http"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi"
)

type UnmarshalMessageFunc func(topic string, request *http.Request) (*message.Message, error)

type Subscriber struct {
	router chi.Router
	server *http.Server
	logger watermill.LoggerAdapter

	unmarshalMessageFunc UnmarshalMessageFunc

	outputChannels     []chan *message.Message
	outputChannelsLock sync.Locker

	closed bool
}

func NewSubscriber(addr string, unmarshalMessageFunc UnmarshalMessageFunc, logger watermill.LoggerAdapter) (*Subscriber, error) {
	r := chi.NewRouter()
	s := &http.Server{Addr: addr, Handler: r}

	return &Subscriber{
		r,
		s,
		logger,
		unmarshalMessageFunc,
		make([]chan *message.Message, 1),
		&sync.Mutex{},
		false,
	}, nil
}

func (s *Subscriber) Subscribe(topic string) (chan *message.Message, error) {
	messages := make(chan *message.Message)

	s.outputChannelsLock.Lock()
	s.outputChannels = append(s.outputChannels, messages)
	s.outputChannelsLock.Unlock()

	baseLogFields := watermill.LogFields{"topic": topic}

	s.router.Post(topic, func(w http.ResponseWriter, r *http.Request) {
		msg, err := s.unmarshalMessageFunc(topic, r)
		if err != nil {
			s.logger.Info("Cannot unmarshal message", baseLogFields.Add(watermill.LogFields{"err": err}))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if msg == nil {
			s.logger.Info("No message returned by unmarshalMessageFunc", baseLogFields)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		logFields := baseLogFields.Add(watermill.LogFields{"message_uuid": msg.UUID})

		s.logger.Trace("Sending msg", logFields)
		messages <- msg

		s.logger.Trace("Waiting for ACK", logFields)
		select {
		case <-msg.Acked():
			s.logger.Trace("Message acknowledged", logFields.Add(watermill.LogFields{"err": err}))
			w.WriteHeader(http.StatusOK)
		case <-msg.Nacked():
			w.WriteHeader(http.StatusInternalServerError)
		case <-r.Context().Done():
			s.logger.Info("Request stopped without ACK received", logFields)
		}
	})

	return messages, nil
}

func (s *Subscriber) RunHTTPServer() error {
	return s.server.ListenAndServe()
}

func (s Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	defer func() {
		for _, ch := range s.outputChannels {
			close(ch)
		}
	}()

	return s.server.Close()
}
