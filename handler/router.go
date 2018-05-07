package handler

import (
	"sync"
	"github.com/pkg/errors"
	sync_internal "github.com/roblaszczak/gooddd/internal/sync"
	"time"
	"github.com/roblaszczak/gooddd/domain"
	"fmt"
)

// todo - rename package
// check: https://en.wikipedia.org/wiki/Event-driven_architecture

type Handler func(msg domain.Event) ([]domain.EventPayload, error)

type Middleware func(h Handler) Handler

type Plugin func(*Router) error

// todo - make it easier to implement
type EventsListener interface {
	Subscribe(topic string) (chan domain.Event, error) // rename to listen? rename to EventsListener??
	Close() error
}

type listenerFactoryFunc func(subscriberName string) (EventsListener, error)

func NewRouter(listenerFactoryFunc listenerFactoryFunc) *Router {
	return &Router{
		listenerFactoryFunc: listenerFactoryFunc,

		subscribers: map[string]*subscriber{},

		messagesWg: &sync.WaitGroup{},

		closeCh: make(chan struct{}),
	}
}

// todo - rename!!
type Router struct {
	listenerFactoryFunc listenerFactoryFunc

	middlewares []Middleware

	plugins []Plugin

	subscribers map[string]*subscriber

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

type subscriber struct {
	name    string
	topic   string
	handler Handler

	subscriberWorkers []subscriberWorker
}

type subscriberWorker struct {
	listener      EventsListener
	eventsChannel chan domain.Event
}

func (r *Router) Subscribe(subscriberName string, topic string, handler Handler) error {
	if _, ok := r.subscribers[subscriberName]; ok {
		return errors.Errorf("subscriber %s already exists", subscriberName)
	}

	r.subscribers[subscriberName] = &subscriber{name: subscriberName, topic: topic, handler: handler}
	return nil
}

func (r *Router) Run() error {
	// todo - defer cleanup & close
	for _, plugin := range r.plugins {
		if err := plugin(r); err != nil {
			return errors.Wrapf(err, "cannot initialize plugin %s", plugin)
		}
	}

	for _, s := range r.subscribers {
		for i := 0; i < 8; i ++ { // todo - config
			listener, err := r.listenerFactoryFunc(s.name)
			if err != nil {
				return errors.Wrap(err, "cannot create listener")
			}

			eventsChannel, err := listener.Subscribe(s.topic)
			if err != nil {
				return errors.Wrapf(err, "cannot subscribe topic %s", s.topic)
			}

			s.subscriberWorkers = append(s.subscriberWorkers, subscriberWorker{listener, eventsChannel})
		}
	}

	for _, s := range r.subscribers {
		for _, w := range s.subscriberWorkers {
			r.messagesWg.Add(1)

			go func(s *subscriber, w subscriberWorker) {
				defer r.messagesWg.Done()

				middlewareHandler := s.handler

				// first added middlewares should be executed first (so should be at the top of call stack)
				for i := len(r.middlewares) - 1; i >= 0; i-- {
					middlewareHandler = r.middlewares[i](middlewareHandler)
				}

				for msg := range w.eventsChannel {
					// todo - handle error and messages
					middlewareHandler(msg)
				}

				// todo - handle errors
				err := w.listener.Close()
				if err != nil {
					panic(err)
				}
			}(s, w)
		}
	}

	<-r.closeCh

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
