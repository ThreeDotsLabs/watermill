package http

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
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
	msg := message.NewMessage(uuid, body)

	metadata := req.Header.Get(HeaderMetadata)
	if metadata != "" {
		err = json.Unmarshal([]byte(metadata), &msg.Metadata)
		if err != nil {
			return nil, errors.Wrap(err, "could not unmarshal metadata from request")
		}
	}

	return msg, nil
}

type SubscriberConfig struct {
	Router               chi.Router
	UnmarshalMessageFunc UnmarshalMessageFunc
}

func (s *SubscriberConfig) setDefaults() {
	if s.Router == nil {
		s.Router = chi.NewRouter()
	}

	if s.UnmarshalMessageFunc == nil {
		s.UnmarshalMessageFunc = DefaultUnmarshalMessageFunc
	}
}

// Subscriber can subscribe to HTTP requests and create Watermill's messages based on them.
type Subscriber struct {
	config SubscriberConfig

	server *http.Server

	address  net.Addr
	addrLock sync.RWMutex

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
	s := &http.Server{Addr: addr, Handler: config.Router}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Subscriber{
		config:             config,
		server:             s,
		logger:             logger,
		outputChannels:     make([]chan *message.Message, 0),
		outputChannelsLock: &sync.Mutex{},
	}, nil
}

// Subscribe adds HTTP handler which will listen in provided url for messages.
//
// Subscribe needs to be called before `StartHTTPServer`.
//
// When request is sent, it will wait for the `Ack`. When Ack is received 200 HTTP status wil be sent.
// When Nack is sent, 500 HTTP status will be sent.
func (s *Subscriber) Subscribe(ctx context.Context, url string) (<-chan *message.Message, error) {
	messages := make(chan *message.Message)

	s.outputChannelsLock.Lock()
	s.outputChannels = append(s.outputChannels, messages)
	s.outputChannelsLock.Unlock()

	baseLogFields := watermill.LogFields{"url": url, "provider": ProviderName}

	if !strings.HasPrefix(url, "/") {
		url = "/" + url
	}

	s.config.Router.Post(url, func(w http.ResponseWriter, r *http.Request) {
		msg, err := s.config.UnmarshalMessageFunc(url, r)

		ctx, cancelCtx := context.WithCancel(ctx)
		msg.SetContext(ctx)
		defer cancelCtx()

		if err != nil {
			s.logger.Info("Cannot unmarshal message", baseLogFields.Add(watermill.LogFields{"err": err}))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if msg == nil {
			s.logger.Info("No message returned by UnmarshalMessageFunc", baseLogFields)
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
			s.logger.Trace("Message nacked", logFields.Add(watermill.LogFields{"err": err}))
			w.WriteHeader(http.StatusInternalServerError)
		case <-r.Context().Done():
			s.logger.Info("Request stopped without ACK received", logFields)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	return messages, nil
}

// StartHTTPServer starts http server.
// It must be called after all Subscribe calls have completed.
// Just like http.Server.Serve(), it returns http.ErrServerClosed after the server's been closed.
// https://golang.org/pkg/net/http/#Server.Serve
func (s *Subscriber) StartHTTPServer() error {
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.addrLock.Lock()
	s.address = listener.Addr()
	s.addrLock.Unlock()

	return s.server.Serve(listener)
}

// Addr returns the server address or nil if the server isn't running.
func (s *Subscriber) Addr() net.Addr {
	s.addrLock.RLock()
	defer s.addrLock.RUnlock()

	return s.address
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
