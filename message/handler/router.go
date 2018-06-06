package handler

import (
	"sync"
	"github.com/pkg/errors"
	sync_internal "github.com/roblaszczak/gooddd/internal/sync"
	"time"
	"fmt"
	"github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd"
)

// todo - rename package
// check: https://en.wikipedia.org/wiki/Event-driven_architecture

type HandlerFunc func(msg message.Message) (producedMessages []message.Message, err error)

type Middleware func(h HandlerFunc) HandlerFunc

type Plugin func(*Router) error

func NewRouter(serverName string, subscriber message.Subscriber, publisher message.Publisher) *Router {
	// todo -validate server name

	return &Router{
		serverName: serverName,

		subscriber: subscriber,
		publisher:  publisher,

		handlers: map[string]*handler{},

		handlersWg:        &sync.WaitGroup{},
		runningHandlersWg: &sync.WaitGroup{},

		closeCh: make(chan struct{}),

		Logger: gooddd.NopLogger{},
	}
}

// todo - rename!!
type Router struct {
	serverName string

	subscriber message.Subscriber // todo - rename? or rename subscribers?
	publisher  message.Publisher

	middlewares []Middleware

	plugins []Plugin

	handlers map[string]*handler

	handlersWg        *sync.WaitGroup
	runningHandlersWg *sync.WaitGroup

	closeCh chan struct{}

	Logger gooddd.LoggerAdapter

	running bool
}

// todo - rename
// todo - doc that order matters
func (r *Router) AddMiddleware(m ...Middleware) {
	r.Logger.Debug("Adding middlewares", gooddd.LogFields{"count": fmt.Sprintf("%d", len(m))})

	// todo - use
	r.middlewares = append(r.middlewares, m...)
}

func (r *Router) AddPlugin(p ...Plugin) {
	r.Logger.Debug("Adding plugins", gooddd.LogFields{"count": fmt.Sprintf("%d", len(p))})

	// todo - use
	r.plugins = append(r.plugins, p...)
}

type handler struct {
	name        string
	topic       string
	handlerFunc HandlerFunc

	messagesCh chan message.Message

	metadata message.SubscriberMetadata // todo - rename it?
}

func (r *Router) Subscribe(subscriberName string, topic string, handlerFunc HandlerFunc) error {
	r.Logger.Info("Adding subscriber", gooddd.LogFields{
		"subscriber_name": subscriberName,
		"topic":           topic,
	})

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
	if r.running {
		return errors.New("router is already running")
	}
	r.running = true

	// todo - defer cleanup & close
	r.Logger.Debug("Loading plugins", nil)
	for _, plugin := range r.plugins {
		if err := plugin(r); err != nil {
			return errors.Wrapf(err, "cannot initialize plugin %s", plugin)
		}
	}

	for _, s := range r.handlers {
		r.Logger.Debug("Subscribing to topic", gooddd.LogFields{
			"subscriber_name": s.name,
			"topic":           s.topic,
		})

		messages, err := r.subscriber.Subscribe(s.topic, s.metadata)
		if err != nil {
			return errors.Wrapf(err, "cannot subscribe topic %s", s.topic)
		}

		s.messagesCh = messages
	}

	for i := range r.handlers {
		r.handlersWg.Add(1)

		go func(s *handler) {
			r.Logger.Info("Starting handler", gooddd.LogFields{
				"subscriber_name": s.name,
				"topic":           s.topic,
			})

			middlewareHandler := s.handlerFunc

			// first added middlewares should be executed first (so should be at the top of call stack)
			for i := len(r.middlewares) - 1; i >= 0; i-- {
				middlewareHandler = r.middlewares[i](middlewareHandler)
			}

			for msg := range s.messagesCh {
				r.runningHandlersWg.Add(1)

				go func(msg message.Message) {
					defer r.runningHandlersWg.Done()

					msgFields := gooddd.LogFields{"message_uuid": msg.UUID()}

					r.Logger.Trace("Received message", msgFields)

					producedMessages, err := middlewareHandler(msg)
					if err != nil {
						// todo - what to do with it?
						fmt.Println(err)
						return
					}

					if len(producedMessages) > 0 {
						r.Logger.Trace("Sending produced messages", msgFields.Add(gooddd.LogFields{
							"produced_messages_count": len(producedMessages),
						}))

						// todo - set topic
						if err := r.publisher.Publish(producedMessages); err != nil {
							// todo - what to do with it?
							fmt.Println(err)
							return
						}
					}

					r.Logger.Trace("Message processed", msgFields)
				}(msg)
			}

			r.handlersWg.Done()
			r.Logger.Info("Handler stopped", gooddd.LogFields{
				"subscriber_name": s.name,
				"topic":           s.topic,
			})
			// todo - wait for it
		}(r.handlers[i])
	}

	<-r.closeCh

	r.Logger.Debug("Waiting for subscriber to close", nil)
	err := r.subscriber.Close()
	if err != nil {
		return errors.Wrap(err, "cannot close handler")
	}
	r.Logger.Debug("Subscriber closed", nil)

	r.Logger.Info("Router closed", nil)

	timeout := time.Second * 10

	r.Logger.Info("Waiting for messages", gooddd.LogFields{
		"timeout": timeout,
	})

	// todo - add timeout to all wg's & subscriber close
	timeouted := sync_internal.WaitGroupTimeout(r.handlersWg, timeout) // todo - config
	if timeouted {
		// todo - what to do with it?
		r.Logger.Info("Timeout", nil)
	} else {
		r.Logger.Info("All messages processed", nil)
	}

	return nil
}

func (r *Router) Close() error {
	r.Logger.Info("Closing router", nil)
	r.closeCh <- struct{}{}

	return nil
}
