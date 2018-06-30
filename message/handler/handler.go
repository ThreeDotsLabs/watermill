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

type HandlerFunc func(msg message.Message) (producedMessages []message.Message, err error)

type Middleware func(h HandlerFunc) HandlerFunc

type Plugin func(*Handler) error

type Config struct {
	ServerName         string
	PublishEventsTopic string

	CloseTimeout time.Duration
}

func (c Config) Validate() error {
	if c.ServerName == "" {
		return errors.New("empty ServerName")
	}
	if c.PublishEventsTopic == "" {
		// todo - create router without PublishEventsTopic
		return errors.New("empty PublishEventsTopic")
	}

	return nil
}

func (c *Config) setDefaults() {
	if c.CloseTimeout == 0 {
		c.CloseTimeout = time.Second * 30
	}
}

func NewHandler(config *Config, subscriber message.Subscriber, publisher message.Publisher) (*Handler, error) {
	if config == nil {
		return nil, errors.New("missing config")
	}

	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &Handler{
		config: config,

		subscriber: subscriber,
		publisher:  publisher,

		handlers: map[string]*handler{},

		handlersWg:        &sync.WaitGroup{},
		runningHandlersWg: &sync.WaitGroup{},

		closeCh: make(chan struct{}),

		Logger: gooddd.NopLogger{},
	}, nil
}

type Handler struct {
	config *Config

	subscriber message.Subscriber
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

// AddMiddleware adds a new middleware to the router.
//
// The order of middlewares matters. Middleware added at the beginning is executed first.
func (r *Handler) AddMiddleware(m ...Middleware) {
	r.Logger.Debug("Adding middlewares", gooddd.LogFields{"count": fmt.Sprintf("%d", len(m))})

	r.middlewares = append(r.middlewares, m...)
}

func (r *Handler) AddPlugin(p ...Plugin) {
	r.Logger.Debug("Adding plugins", gooddd.LogFields{"count": fmt.Sprintf("%d", len(p))})

	r.plugins = append(r.plugins, p...)
}

type handler struct {
	name        string
	topic       string
	handlerFunc HandlerFunc

	messagesCh chan message.Message
}

func (r *Handler) Subscribe(subscriberName string, topic string, handlerFunc HandlerFunc) error {
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
	}
	return nil
}

func (r *Handler) Run() error {
	if r.running {
		return errors.New("handler is already running")
	}
	r.running = true

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

		messages, err := r.subscriber.Subscribe(s.topic)
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
						msg.Error(err)
						return
					}

					if len(producedMessages) > 0 {
						r.Logger.Trace("Sending produced messages", msgFields.Add(gooddd.LogFields{
							"produced_messages_count": len(producedMessages),
						}))

						if err := r.publisher.Publish(r.config.PublishEventsTopic, producedMessages); err != nil {
							// todo - how to deal with it better?
							r.Logger.Error("cannot publish message", err, msgFields.Add(gooddd.LogFields{
								"not_sent_message": fmt.Sprintf("%#v", producedMessages),
							}))
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
		}(r.handlers[i])
	}

	<-r.closeCh

	r.Logger.Debug("Waiting for subscriber to close", nil)
	if err := r.subscriber.CloseSubscriber(); err != nil {
		return errors.Wrap(err, "cannot close handler")
	}
	r.Logger.Debug("Subscriber closed", nil)

	r.Logger.Debug("Waiting for publisher to close", nil)
	if err := r.publisher.ClosePublisher(); err != nil {
		return errors.Wrap(err, "cannot close handler")
	}
	r.Logger.Debug("Publisher closed", nil)

	r.Logger.Info("Waiting for messages", gooddd.LogFields{
		"timeout": r.config.CloseTimeout,
	})
	timeouted := sync_internal.WaitGroupTimeout(r.handlersWg, r.config.CloseTimeout)
	if timeouted {
		return errors.New("handler close timeouted")
	}

	r.Logger.Info("All messages processed", nil)
	return nil
}

func (r *Handler) Close() error {
	r.Logger.Info("Closing handler", nil)
	r.closeCh <- struct{}{}

	return nil
}
