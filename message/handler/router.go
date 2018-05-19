package handler

import (
	"sync"
	"github.com/pkg/errors"
	sync_internal "github.com/roblaszczak/gooddd/internal/sync"
	"time"
	"fmt"
	"github.com/roblaszczak/gooddd/message"
)

// todo - rename package
// check: https://en.wikipedia.org/wiki/Event-driven_architecture

type HandlerFunc func(msg *message.Message) (returnMsgPayloads []message.Payload, err error)

type Middleware func(h HandlerFunc) HandlerFunc

type Plugin func(*Router) error

func NewRouter(serverName string, subscriber message.Subscriber) *Router {
	return &Router{
		serverName: serverName,

		subscriber: subscriber,

		handlers: map[string]*handler{},

		messagesWg: &sync.WaitGroup{},

		closeCh: make(chan struct{}),
	}
}

// todo - rename!!
type Router struct {
	serverName string

	subscriber message.Subscriber // todo - rename? or rename subscribers?

	middlewares []Middleware

	plugins []Plugin

	handlers map[string]*handler

	messagesWg *sync.WaitGroup

	closeCh chan struct{}
}

// todo - rename
// todo - doc that order matters
func (r *Router) AddMiddleware(m ...Middleware) {
	// todo - use
	r.middlewares = append(r.middlewares, m...)
}

func (r *Router) AddPlugin(p ...Plugin) {
	// todo - use
	r.plugins = append(r.plugins, p...)
}

type handler struct {
	name        string
	topic       string
	handlerFunc HandlerFunc

	messagesCh chan *message.Message

	metadata message.SubscriberMetadata // todo - rename it?
}

func (r *Router) Subscribe(subscriberName string, topic string, handlerFunc HandlerFunc) error {
	if _, ok := r.handlers[subscriberName]; ok {
		return errors.Errorf("handler %s already exists", subscriberName)
	}

	r.handlers[subscriberName] = &handler{
		name:        subscriberName,
		topic:       topic,
		handlerFunc: handlerFunc,

		metadata: message.SubscriberMetadata{
			SubscriberName: subscriberName,
			ServerName:     r.serverName,
			Hostname:       "localhost", // todo
		},
	}
	return nil
}

func (r *Router) Run() error {
	// todo - defer cleanup & close
	for _, plugin := range r.plugins {
		if err := plugin(r); err != nil {
			return errors.Wrapf(err, "cannot initialize plugin %s", plugin)
		}
	}

	for _, s := range r.handlers {
		messages, err := r.subscriber.Subscribe(s.topic, s.metadata)
		if err != nil {
			return errors.Wrapf(err, "cannot subscribe topic %s", s.topic)
		}

		s.messagesCh = messages
	}

	for i := range r.handlers {
		go func(messages <-chan *message.Message, handler HandlerFunc) {
			middlewareHandler := handler

			// first added middlewares should be executed first (so should be at the top of call stack)
			for i := len(r.middlewares) - 1; i >= 0; i-- {
				middlewareHandler = r.middlewares[i](middlewareHandler)
			}

			for msg := range messages {
				r.messagesWg.Add(1)
				go func() {
					defer r.messagesWg.Done()
					middlewareHandler(msg)
				}()
			}
		}(r.handlers[i].messagesCh, r.handlers[i].handlerFunc)
	}

	<-r.closeCh
	err := r.subscriber.Close()
	if err != nil {
		return errors.Wrap(err, "cannot close handler")
	}

	return nil
}

func (r *Router) Close() error {
	fmt.Println("closing")

	go func() {
		// todo - replace with context??
		for {
			r.closeCh <- struct{}{}
		}
	}()

	sync_internal.WaitGroupTimeout(r.messagesWg, time.Second*10) // todo - config

	return nil
}
