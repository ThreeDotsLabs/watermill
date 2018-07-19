package http

import (
	"github.com/roblaszczak/gooddd/message"
	"github.com/go-chi/chi"
	"net/http"
	"github.com/roblaszczak/gooddd"
	"sync"
)

type UnmarshalMessageFunc func(topic string, request *http.Request) (message.Message, error)

type Subscriber struct {
	router chi.Router
	server *http.Server
	logger gooddd.LoggerAdapter

	unmarshalMessageFunc UnmarshalMessageFunc

	outputChannels     []chan message.Message
	outputChannelsLock sync.Locker
}

// todo - test
func NewSubscriber(addr string, unmarshalMessageFunc UnmarshalMessageFunc, logger gooddd.LoggerAdapter) (message.Subscriber, error) {
	r := chi.NewRouter()
	s := &http.Server{Addr: addr, Handler: r}

	return &Subscriber{
		r,
		s,
		logger,
		unmarshalMessageFunc,
		make([]chan message.Message, 1),
		&sync.Mutex{},
	}, nil
}

func (s *Subscriber) Subscribe(topic string, consumerGroup message.ConsumerGroup) (chan message.Message, error) {
	messages := make(chan message.Message)

	s.outputChannelsLock.Lock()
	s.outputChannels = append(s.outputChannels, messages)
	s.outputChannelsLock.Unlock()

	baseLogFields := gooddd.LogFields{"topic": topic}

	s.router.Post(topic, func(w http.ResponseWriter, r *http.Request) {
		msg, err := s.unmarshalMessageFunc(topic, r)
		if err != nil {
			s.logger.Info("Cannot unmarshal message", baseLogFields.Add(gooddd.LogFields{"err": err}))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if msg == nil {
			s.logger.Info("No message returned by unmarshalMessageFunc", baseLogFields)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		logFields := baseLogFields.Add(gooddd.LogFields{"message_id": msg.UUID()})

		s.logger.Trace("Sending msg", logFields)
		messages <- msg

		s.logger.Trace("Waiting for ACK", logFields)
		select {
		case err := <-msg.Acknowledged():
			s.logger.Trace("Message acknowledged", logFields.Add(gooddd.LogFields{"err": err}))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case <-r.Context().Done():
			s.logger.Info("Request stopped without ACK received", logFields)
		}
	})

	// todo - how to handle errors?
	s.server.Close()
	go s.server.ListenAndServe()

	return messages, nil
}

func (s Subscriber) CloseSubscriber() error {
	defer func() {
		for _, ch := range s.outputChannels {
			close(ch)
		}
	}()

	return s.server.Close()
}
