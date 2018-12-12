package http

import (
	"net/http"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi"
)

type UnmarshalMessageFunc func(topic string, request *http.Request) (*message.Message, error)

// Subscriber can subscribe to HTTP requests and create Watermill's messages based on them.
type Subscriber struct {
	router chi.Router
	server *http.Server
	logger watermill.LoggerAdapter

	unmarshalMessageFunc UnmarshalMessageFunc

	outputChannels     []chan *message.Message
	outputChannelsLock sync.Locker

	closed bool
}

// NewSubscriber creates new Subscriber.
//
// addr is TCP address to listen on
//
// unmarshalMessageFunc is function which converts HTTP request to Watermill's message.
//
// logger is Watermill's logger.
func NewSubscriber(addr string, unmarshalMessageFunc UnmarshalMessageFunc, logger watermill.LoggerAdapter) (*Subscriber, error) {
	r := chi.NewRouter()
	s := &http.Server{Addr: addr, Handler: r}

	return &Subscriber{
		r,
		s,
		logger,
		unmarshalMessageFunc,
		make([]chan *message.Message, 0),
		&sync.Mutex{},
		false,
	}, nil
}

// Subscribe adds HTTP handler which will listen in provided url for messages.
//
// Subscribe needs to be called before `StartHTTPServer`.
//
// When request is sent, it will wait for the `Ack`. When Ack is received 200 HTTP status wil be sent.
// When Nack is sent, 500 HTTP status will be sent.
func (s *Subscriber) Subscribe(url string) (chan *message.Message, error) {
	messages := make(chan *message.Message)

	s.outputChannelsLock.Lock()
	s.outputChannels = append(s.outputChannels, messages)
	s.outputChannelsLock.Unlock()

	baseLogFields := watermill.LogFields{"url": url}

	s.router.Post(url, func(w http.ResponseWriter, r *http.Request) {
		msg, err := s.unmarshalMessageFunc(url, r)
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

// StartHTTPServer starts http server.
// StartHTTPServer must be called after all subscribe function are called.
func (s *Subscriber) StartHTTPServer() error {
	return s.server.ListenAndServe()
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	if err := s.server.Close(); err != nil {
		return err
	}

	for _, ch := range s.outputChannels {
		close(ch)
	}

	return nil
}
