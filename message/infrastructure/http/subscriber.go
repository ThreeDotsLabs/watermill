package http

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi"
)

type UnmarshalMessageFunc func(topic string, request *http.Request) (*message.Message, error)

// DefaultUnmarshalMessageFunc retrieves the UUID and Metadata from request headers,
// as encoded by DefaultMarshalMessageFunc.
func DefaultUnmarshalMessageFunc(topic string, req *http.Request) (*message.Message, error) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	uuid := req.Header.Get(HeaderUUID)
	if uuid == "" {
		return nil, errors.New("No UUID encoded in header")
	}

	msg := message.NewMessage(uuid, body)

	metadata := req.Header.Get(HeaderMetadata)
	err = json.Unmarshal([]byte(metadata), &msg.Metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not unmarshal metadata from request")
	}

	return msg, nil
}

type SubscriberConfig struct {
	router               chi.Router
	unmarshalMessageFunc UnmarshalMessageFunc
}

func (s *SubscriberConfig) setDefaults() {
	if s.router == nil {
		s.router = chi.NewRouter()
	}

	if s.unmarshalMessageFunc == nil {
		s.unmarshalMessageFunc = DefaultUnmarshalMessageFunc
	}
}

// Subscriber can subscribe to HTTP requests and create Watermill's messages based on them.
type Subscriber struct {
	config SubscriberConfig

	server *http.Server
	logger watermill.LoggerAdapter

	outputChannels     []chan *message.Message
	outputChannelsLock sync.Locker

	closed bool
}

// NewSubscriber creates new Subscriber.
//
// addr is TCP address to listen on
//
// logger is Watermill's logger.
func NewSubscriber(addr string, config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()
	s := &http.Server{Addr: addr, Handler: config.router}

	return &Subscriber{
		config,
		s,
		logger,
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

	baseLogFields := watermill.LogFields{"url": url, "provider": ProviderName}

	if !strings.HasPrefix(url, "/") {
		url = "/" + url
	}

	s.config.router.Post(url, func(w http.ResponseWriter, r *http.Request) {
		msg, err := s.config.unmarshalMessageFunc(url, r)
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
func (s *Subscriber) StartHTTPServer() (chan error, error) {
	errChan := make(chan error, 1)
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return nil, err
	}

	go func() {
		err := s.server.Serve(listener)
		errChan <- err
	}()
	return errChan, nil
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
