package message

import (
	"sync"
	"github.com/pkg/errors"
	sync_internal "github.com/roblaszczak/gooddd/internal/sync"
	"time"
	"fmt"
	"github.com/roblaszczak/gooddd"
)

type HandlerFunc func(msg ConsumedMessage) ([]ProducedMessage, error)

type HandlerMiddleware func(h HandlerFunc) HandlerFunc

type RouterPlugin func(*Router) error

type GenerateConsumerGroup func(serverName, handlerName string) ConsumerGroup

func DefaultGenerateConsumerGroup(serverName, handlerName string) ConsumerGroup {
	return ConsumerGroup(fmt.Sprintf("%s_%s", serverName, handlerName))
}

type RouterConfig struct {
	ServerName         string
	PublishEventsTopic string

	CloseTimeout time.Duration

	GenerateConsumerGroupFunc GenerateConsumerGroup
}

func (c *RouterConfig) setDefaults() {
	if c.CloseTimeout == 0 {
		c.CloseTimeout = time.Second * 30
	}
	if c.GenerateConsumerGroupFunc == nil {
		c.GenerateConsumerGroupFunc = DefaultGenerateConsumerGroup
	}
}

func (c RouterConfig) Validate() error {
	if c.ServerName == "" {
		return errors.New("empty ServerName")
	}
	if c.PublishEventsTopic == "" {
		// todo - create router without PublishEventsTopic
		return errors.New("empty PublishEventsTopic")
	}

	return nil
}

func NewRouter(config RouterConfig, subscriber Subscriber, publisher Publisher) (*Router, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &Router{
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

type Router struct {
	config RouterConfig

	subscriber Subscriber
	publisher  Publisher

	middlewares []HandlerMiddleware

	plugins []RouterPlugin

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
func (r *Router) AddMiddleware(m ...HandlerMiddleware) {
	r.Logger.Debug("Adding middlewares", gooddd.LogFields{"count": fmt.Sprintf("%d", len(m))})

	r.middlewares = append(r.middlewares, m...)
}

func (r *Router) AddPlugin(p ...RouterPlugin) {
	r.Logger.Debug("Adding plugins", gooddd.LogFields{"count": fmt.Sprintf("%d", len(p))})

	r.plugins = append(r.plugins, p...)
}

type handler struct {
	name        string
	topic       string
	handlerFunc HandlerFunc

	messagesCh chan ConsumedMessage
}

func (r *Router) AddHandler(handlerName string, topic string, handlerFunc HandlerFunc) error {
	r.Logger.Info("Adding subscriber", gooddd.LogFields{
		"handler_name": handlerName,
		"topic":        topic,
	})

	if _, ok := r.handlers[handlerName]; ok {
		return errors.Errorf("handler %s already exists", handlerName)
	}

	r.handlers[handlerName] = &handler{
		name:        handlerName,
		topic:       topic,
		handlerFunc: handlerFunc,
	}

	return nil
}

func (r *Router) Run() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("panic recovered: %#v", r)
			return
		}
	}()

	if r.running {
		return errors.New("router is already running")
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

		messages, err := r.subscriber.Subscribe(
			s.topic,
			r.config.GenerateConsumerGroupFunc(r.config.ServerName, s.name),
		)
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

				go func(msg ConsumedMessage) {
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
			r.Logger.Info("Subscriber stopped", gooddd.LogFields{
				"subscriber_name": s.name,
				"topic":           s.topic,
			})
		}(r.handlers[i])
	}

	<-r.closeCh

	r.Logger.Debug("Waiting for subscriber to close", nil)
	if err := r.subscriber.CloseSubscriber(); err != nil {
		return errors.Wrap(err, "cannot close router")
	}
	r.Logger.Debug("Subscriber closed", nil)

	r.Logger.Debug("Waiting for publisher to close", nil)
	if err := r.publisher.ClosePublisher(); err != nil {
		return errors.Wrap(err, "cannot close router")
	}
	r.Logger.Debug("Publisher closed", nil)

	r.Logger.Info("Waiting for messages", gooddd.LogFields{
		"timeout": r.config.CloseTimeout,
	})

	r.Logger.Info("All messages processed", nil)
	return nil
}

func (r *Router) Close() error {
	r.Logger.Info("Closing router", nil)
	defer r.Logger.Info("Router closed", nil)
	r.closeCh <- struct{}{}

	timeouted := sync_internal.WaitGroupTimeout(r.handlersWg, r.config.CloseTimeout)
	if timeouted {
		return errors.New("router close timeouted")
	}

	return nil
}
