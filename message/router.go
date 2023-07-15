package message

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal"
	sync_internal "github.com/ThreeDotsLabs/watermill/pubsub/sync"
)

var (
	// ErrOutputInNoPublisherHandler happens when a handler func returned some messages in a no-publisher handler.
	// todo: maybe change the handler func signature in no-publisher handler so that there's no possibility for this
	ErrOutputInNoPublisherHandler = errors.New("returned output messages in a handler without publisher")
)

// HandlerFunc is function called when message is received.
//
// msg.Ack() is called automatically when HandlerFunc doesn't return error.
// When HandlerFunc returns error, msg.Nack() is called.
// When msg.Ack() was called in handler and HandlerFunc returns error,
// msg.Nack() will be not sent because Ack was already sent.
//
// HandlerFunc's are executed parallel when multiple messages was received
// (because msg.Ack() was sent in HandlerFunc or Subscriber supports multiple consumers).
type HandlerFunc func(msg *Message) ([]*Message, error)

// NoPublishHandlerFunc is HandlerFunc alternative, which doesn't produce any messages.
type NoPublishHandlerFunc func(msg *Message) error

// PassthroughHandler is a handler that passes the message unchanged from the subscriber to the publisher.
var PassthroughHandler HandlerFunc = func(msg *Message) ([]*Message, error) {
	return []*Message{msg}, nil
}

// HandlerMiddleware allows us to write something like decorators to HandlerFunc.
// It can execute something before handler (for example: modify consumed message)
// or after (modify produced messages, ack/nack on consumed message, handle errors, logging, etc.).
//
// It can be attached to the router by using `AddMiddleware` method.
//
// Example:
//
//	func ExampleMiddleware(h message.HandlerFunc) message.HandlerFunc {
//		return func(message *message.Message) ([]*message.Message, error) {
//			fmt.Println("executed before handler")
//			producedMessages, err := h(message)
//			fmt.Println("executed after handler")
//
//			return producedMessages, err
//		}
//	}
type HandlerMiddleware func(h HandlerFunc) HandlerFunc

// RouterPlugin is function which is executed on Router start.
type RouterPlugin func(*Router) error

// PublisherDecorator wraps the underlying Publisher, adding some functionality.
type PublisherDecorator func(pub Publisher) (Publisher, error)

// SubscriberDecorator wraps the underlying Subscriber, adding some functionality.
type SubscriberDecorator func(sub Subscriber) (Subscriber, error)

// RouterConfig holds the Router's configuration options.
type RouterConfig struct {
	// CloseTimeout determines how long router should work for handlers when closing.
	CloseTimeout time.Duration
}

func (c *RouterConfig) setDefaults() {
	if c.CloseTimeout == 0 {
		c.CloseTimeout = time.Second * 30
	}
}

// Validate returns Router configuration error, if any.
func (c RouterConfig) Validate() error {
	return nil
}

// NewRouter creates a new Router with given configuration.
func NewRouter(config RouterConfig, logger watermill.LoggerAdapter) (*Router, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Router{
		config: config,

		handlers: map[string]*handler{},

		handlersWg: &sync.WaitGroup{},

		runningHandlersWg:     &sync.WaitGroup{},
		runningHandlersWgLock: &sync.Mutex{},

		handlerAdded: make(chan struct{}),

		handlersLock: &sync.RWMutex{},

		closingInProgressCh: make(chan struct{}),
		closedCh:            make(chan struct{}),

		logger: logger,

		running: make(chan struct{}),
	}, nil
}

type middleware struct {
	Handler       HandlerMiddleware
	HandlerName   string
	IsRouterLevel bool
}

// Router is responsible for handling messages from subscribers using provided handler functions.
//
// If the handler function returns a message, the message is published with the publisher.
// You can use middlewares to wrap handlers with common logic like logging, instrumentation, etc.
type Router struct {
	config RouterConfig

	middlewares []middleware

	plugins []RouterPlugin

	handlers     map[string]*handler
	handlersLock *sync.RWMutex

	handlersWg *sync.WaitGroup

	runningHandlersWg     *sync.WaitGroup
	runningHandlersWgLock *sync.Mutex

	handlerAdded chan struct{}

	closingInProgressCh chan struct{}
	closedCh            chan struct{}
	closed              bool
	closedLock          sync.Mutex

	logger watermill.LoggerAdapter

	publisherDecorators  []PublisherDecorator
	subscriberDecorators []SubscriberDecorator

	isRunning bool
	running   chan struct{}
}

// Logger returns the Router's logger.
func (r *Router) Logger() watermill.LoggerAdapter {
	return r.logger
}

// AddMiddleware adds a new middleware to the router.
//
// The order of middleware matters. Middleware added at the beginning is executed first.
func (r *Router) AddMiddleware(m ...HandlerMiddleware) {
	r.logger.Debug("Adding middleware", watermill.LogFields{"count": fmt.Sprintf("%d", len(m))})

	r.addRouterLevelMiddleware(m...)
}

func (r *Router) addRouterLevelMiddleware(m ...HandlerMiddleware) {
	for _, handlerMiddleware := range m {
		middleware := middleware{
			Handler:       handlerMiddleware,
			HandlerName:   "",
			IsRouterLevel: true,
		}
		r.middlewares = append(r.middlewares, middleware)
	}
}

func (r *Router) addHandlerLevelMiddleware(handlerName string, m ...HandlerMiddleware) {
	for _, handlerMiddleware := range m {
		middleware := middleware{
			Handler:       handlerMiddleware,
			HandlerName:   handlerName,
			IsRouterLevel: false,
		}
		r.middlewares = append(r.middlewares, middleware)
	}
}

// AddPlugin adds a new plugin to the router.
// Plugins are executed during startup of the router.
//
// A plugin can, for example, close the router after SIGINT or SIGTERM is sent to the process (SignalsHandler plugin).
func (r *Router) AddPlugin(p ...RouterPlugin) {
	r.logger.Debug("Adding plugins", watermill.LogFields{"count": fmt.Sprintf("%d", len(p))})

	r.plugins = append(r.plugins, p...)
}

// AddPublisherDecorators wraps the router's Publisher.
// The first decorator is the innermost, i.e. calls the original publisher.
func (r *Router) AddPublisherDecorators(dec ...PublisherDecorator) {
	r.logger.Debug("Adding publisher decorators", watermill.LogFields{"count": fmt.Sprintf("%d", len(dec))})

	r.publisherDecorators = append(r.publisherDecorators, dec...)
}

// AddSubscriberDecorators wraps the router's Subscriber.
// The first decorator is the innermost, i.e. calls the original subscriber.
func (r *Router) AddSubscriberDecorators(dec ...SubscriberDecorator) {
	r.logger.Debug("Adding subscriber decorators", watermill.LogFields{"count": fmt.Sprintf("%d", len(dec))})

	r.subscriberDecorators = append(r.subscriberDecorators, dec...)
}

// Handlers returns all registered handlers.
func (r *Router) Handlers() map[string]HandlerFunc {
	handlers := map[string]HandlerFunc{}

	for handlerName, handler := range r.handlers {
		handlers[handlerName] = handler.handlerFunc
	}

	return handlers
}

// DuplicateHandlerNameError is sent in a panic when you try to add a second handler with the same name.
type DuplicateHandlerNameError struct {
	HandlerName string
}

func (d DuplicateHandlerNameError) Error() string {
	return fmt.Sprintf("handler with name %s already exists", d.HandlerName)
}

// AddHandler adds a new handler.
//
// handlerName must be unique. For now, it is used only for debugging.
//
// subscribeTopic is a topic from which handler will receive messages.
//
// publishTopic is a topic to which router will produce messages returned by handlerFunc.
// When handler needs to publish to multiple topics,
// it is recommended to just inject Publisher to Handler or implement middleware
// which will catch messages and publish to topic based on metadata for example.
//
// If handler is added while router is already running, you need to explicitly call RunHandlers().
func (r *Router) AddHandler(
	handlerName string,
	subscribeTopic string,
	subscriber Subscriber,
	publishTopic string,
	publisher Publisher,
	handlerFunc HandlerFunc,
) *Handler {
	r.logger.Info("Adding handler", watermill.LogFields{
		"handler_name": handlerName,
		"topic":        subscribeTopic,
	})

	r.handlersLock.Lock()
	defer r.handlersLock.Unlock()

	if _, ok := r.handlers[handlerName]; ok {
		panic(DuplicateHandlerNameError{handlerName})
	}

	publisherName, subscriberName := internal.StructName(publisher), internal.StructName(subscriber)

	newHandler := &handler{
		name:   handlerName,
		logger: r.logger,

		subscriber:     subscriber,
		subscribeTopic: subscribeTopic,
		subscriberName: subscriberName,

		publisher:     publisher,
		publishTopic:  publishTopic,
		publisherName: publisherName,

		handlerFunc: handlerFunc,

		runningHandlersWg:     r.runningHandlersWg,
		runningHandlersWgLock: r.runningHandlersWgLock,

		messagesCh:     nil,
		routersCloseCh: r.closingInProgressCh,

		startedCh: make(chan struct{}),
	}

	r.handlersWg.Add(1)
	r.handlers[handlerName] = newHandler

	select {
	case r.handlerAdded <- struct{}{}:
	default:
		// closeWhenAllHandlersStopped is not always waiting for handlerAdded
	}

	return &Handler{
		router:  r,
		handler: newHandler,
	}
}

// AddNoPublisherHandler adds a new handler.
// This handler cannot return messages.
// When message is returned it will occur an error and Nack will be sent.
//
// handlerName must be unique. For now, it is used only for debugging.
//
// subscribeTopic is a topic from which handler will receive messages.
//
// subscriber is Subscriber from which messages will be consumed.
//
// If handler is added while router is already running, you need to explicitly call RunHandlers().
func (r *Router) AddNoPublisherHandler(
	handlerName string,
	subscribeTopic string,
	subscriber Subscriber,
	handlerFunc NoPublishHandlerFunc,
) *Handler {
	handlerFuncAdapter := func(msg *Message) ([]*Message, error) {
		return nil, handlerFunc(msg)
	}

	return r.AddHandler(handlerName, subscribeTopic, subscriber, "", disabledPublisher{}, handlerFuncAdapter)
}

// Run runs all plugins and handlers and starts subscribing to provided topics.
// This call is blocking while the router is running.
//
// When all handlers have stopped (for example, because subscriptions were closed), the router will also stop.
//
// To stop Run() you should call Close() on the router.
//
// ctx will be propagated to all subscribers.
//
// When all handlers are stopped (for example: because of closed connection), Run() will be also stopped.
func (r *Router) Run(ctx context.Context) (err error) {
	if r.isRunning {
		return errors.New("router is already running")
	}
	r.isRunning = true

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.logger.Debug("Loading plugins", nil)
	for _, plugin := range r.plugins {
		if err := plugin(r); err != nil {
			return errors.Wrapf(err, "cannot initialize plugin %v", plugin)
		}
	}

	if err := r.RunHandlers(ctx); err != nil {
		return err
	}

	close(r.running)

	go r.closeWhenAllHandlersStopped(ctx)

	<-r.closingInProgressCh
	cancel()

	r.logger.Info("Waiting for messages", watermill.LogFields{
		"timeout": r.config.CloseTimeout,
	})

	<-r.closedCh

	r.logger.Info("All messages processed", nil)

	return nil
}

// RunHandlers runs all handlers that were added after Run().
// RunHandlers is idempotent, so can be called multiple times safely.
func (r *Router) RunHandlers(ctx context.Context) error {
	if !r.isRunning {
		return errors.New("you can't call RunHandlers on non-running router")
	}

	r.handlersLock.Lock()
	defer r.handlersLock.Unlock()

	r.logger.Info("Running router handlers", watermill.LogFields{"count": len(r.handlers)})

	for name, h := range r.handlers {
		name := name
		h := h

		if h.started {
			continue
		}

		if err := r.decorateHandlerPublisher(h); err != nil {
			return errors.Wrapf(err, "could not decorate publisher of handler %s", name)
		}
		if err := r.decorateHandlerSubscriber(h); err != nil {
			return errors.Wrapf(err, "could not decorate subscriber of handler %s", name)
		}

		r.logger.Debug("Subscribing to topic", watermill.LogFields{
			"subscriber_name": h.name,
			"topic":           h.subscribeTopic,
		})

		ctx, cancel := context.WithCancel(ctx)

		messages, err := h.subscriber.Subscribe(ctx, h.subscribeTopic)
		if err != nil {
			cancel()
			return errors.Wrapf(err, "cannot subscribe topic %s", h.subscribeTopic)
		}

		h.messagesCh = messages
		h.started = true
		close(h.startedCh)

		h.stopFn = cancel
		h.stopped = make(chan struct{})

		go func() {
			defer cancel()

			h.run(ctx, r.middlewares)

			r.handlersWg.Done()
			r.logger.Info("Subscriber stopped", watermill.LogFields{
				"subscriber_name": h.name,
				"topic":           h.subscribeTopic,
			})

			r.handlersLock.Lock()
			delete(r.handlers, name)
			r.handlersLock.Unlock()
		}()
	}
	return nil
}

// closeWhenAllHandlersStopped closed router, when all handlers has stopped,
// because for example all subscriptions are closed.
func (r *Router) closeWhenAllHandlersStopped(ctx context.Context) {
	r.handlersLock.RLock()
	hasHandlers := len(r.handlers) == 0
	r.handlersLock.RUnlock()

	if hasHandlers {
		// in that situation router will be closed immediately (even if they are no routers)
		// let's wait for
		select {
		case <-r.handlerAdded:
			// it should be some handler to track
		case <-r.closedCh:
			// let's avoid goroutine leak
			return
		}
	}

	r.handlersWg.Wait()
	if r.IsClosed() {
		// already closed
		return
	}

	// Only log an error if the context was not canceled, but handlers were stopped.
	select {
	case <-ctx.Done():
	default:
		r.logger.Error("All handlers stopped, closing router", errors.New("all router handlers stopped"), nil)
	}

	if err := r.Close(); err != nil {
		r.logger.Error("Cannot close router", err, nil)
	}
}

// Running is closed when router is running.
// In other words: you can wait till router is running using
//
//	fmt.Println("Starting router")
//	go r.Run(ctx)
//	<- r.Running()
//	fmt.Println("Router is running")
//
// Warning: for historical reasons, this channel is not aware of router closing - the channel will be closed if the router has been running and closed.
func (r *Router) Running() chan struct{} {
	return r.running
}

// IsRunning returns true when router is running.
//
// Warning: for historical reasons, this method is not aware of router closing.
// If you want to know if the router was closed, use IsClosed.
func (r *Router) IsRunning() bool {
	select {
	case <-r.running:
		return true
	default:
		return false
	}
}

// Close gracefully closes the router with a timeout provided in the configuration.
func (r *Router) Close() error {
	r.closedLock.Lock()
	defer r.closedLock.Unlock()

	r.handlersLock.Lock()
	defer r.handlersLock.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	r.logger.Info("Closing router", nil)
	defer r.logger.Info("Router closed", nil)

	close(r.closingInProgressCh)
	defer close(r.closedCh)

	timeouted := r.waitForHandlers()
	if timeouted {
		return errors.New("router close timeout")
	}

	return nil
}

func (r *Router) waitForHandlers() bool {
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		r.handlersWg.Wait()
	}()
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		r.runningHandlersWgLock.Lock()
		defer r.runningHandlersWgLock.Unlock()

		r.runningHandlersWg.Wait()
	}()
	return sync_internal.WaitGroupTimeout(&waitGroup, r.config.CloseTimeout)
}

func (r *Router) IsClosed() bool {
	r.closedLock.Lock()
	defer r.closedLock.Unlock()

	return r.closed
}

type handler struct {
	name   string
	logger watermill.LoggerAdapter

	subscriber     Subscriber
	subscribeTopic string
	subscriberName string

	publisher     Publisher
	publishTopic  string
	publisherName string

	handlerFunc HandlerFunc

	runningHandlersWg     *sync.WaitGroup
	runningHandlersWgLock *sync.Mutex

	messagesCh <-chan *Message

	started   bool
	startedCh chan struct{}

	stopFn         context.CancelFunc
	stopped        chan struct{}
	routersCloseCh chan struct{}
}

func (h *handler) run(ctx context.Context, middlewares []middleware) {
	h.logger.Info("Starting handler", watermill.LogFields{
		"subscriber_name": h.name,
		"topic":           h.subscribeTopic,
	})

	middlewareHandler := h.handlerFunc
	// first added middlewares should be executed first (so should be at the top of call stack)
	for i := len(middlewares) - 1; i >= 0; i-- {
		currentMiddleware := middlewares[i]
		isValidHandlerLevelMiddleware := currentMiddleware.HandlerName == h.name
		if currentMiddleware.IsRouterLevel || isValidHandlerLevelMiddleware {
			middlewareHandler = currentMiddleware.Handler(middlewareHandler)
		}
	}

	go h.handleClose(ctx)

	for msg := range h.messagesCh {
		h.runningHandlersWgLock.Lock()
		h.runningHandlersWg.Add(1)
		h.runningHandlersWgLock.Unlock()

		go h.handleMessage(msg, middlewareHandler)
	}

	if h.publisher != nil {
		h.logger.Debug("Waiting for publisher to close", nil)
		if err := h.publisher.Close(); err != nil {
			h.logger.Error("Failed to close publisher", err, nil)
		}
		h.logger.Debug("Publisher closed", nil)
	}

	h.logger.Debug("Router handler stopped", nil)
	close(h.stopped)
}

// Handler handles Messages.
type Handler struct {
	router  *Router
	handler *handler
}

// AddMiddleware adds new middleware to the specified handler in the router.
//
// The order of middleware matters. Middleware added at the beginning is executed first.
func (h *Handler) AddMiddleware(m ...HandlerMiddleware) {
	handler := h.handler
	handler.logger.Debug("Adding middleware to handler", watermill.LogFields{
		"count":       fmt.Sprintf("%d", len(m)),
		"handlerName": handler.name,
	})

	h.router.addHandlerLevelMiddleware(handler.name, m...)
}

// Started returns channel which is stopped when handler is running.
func (h *Handler) Started() chan struct{} {
	return h.handler.startedCh
}

// Stop stops the handler.
// Stop is asynchronous.
// You can check if handler was stopped with Stopped() function.
func (h *Handler) Stop() {
	if !h.handler.started {
		panic("handler is not started")
	}

	h.handler.stopFn()
}

// Stopped returns channel which is stopped when handler did stop.
func (h *Handler) Stopped() chan struct{} {
	return h.handler.stopped
}

// decorateHandlerPublisher applies the decorator chain to handler's publisher.
// They are applied in reverse order, so that the later decorators use the result of former ones.
func (r *Router) decorateHandlerPublisher(h *handler) error {
	var err error
	pub := h.publisher
	for i := len(r.publisherDecorators) - 1; i >= 0; i-- {
		decorator := r.publisherDecorators[i]
		pub, err = decorator(pub)
		if err != nil {
			return errors.Wrap(err, "could not apply publisher decorator")
		}
	}
	r.handlers[h.name].publisher = pub
	return nil
}

// decorateHandlerSubscriber applies the decorator chain to handler's subscriber.
// They are applied in regular order, so that the later decorators use the result of former ones.
func (r *Router) decorateHandlerSubscriber(h *handler) error {
	var err error
	sub := h.subscriber

	// add values to message context to subscriber
	// it goes before other decorators, so that they may take advantage of these values
	messageTransform := func(msg *Message) {
		if msg != nil {
			h.addHandlerContext(msg)
		}
	}
	sub, err = MessageTransformSubscriberDecorator(messageTransform)(sub)
	if err != nil {
		return errors.Wrapf(err, "cannot wrap subscriber with context decorator")
	}

	for _, decorator := range r.subscriberDecorators {
		sub, err = decorator(sub)
		if err != nil {
			return errors.Wrap(err, "could not apply subscriber decorator")
		}
	}
	r.handlers[h.name].subscriber = sub
	return nil
}

// addHandlerContext enriches the contex with values that are relevant within this handler's context.
func (h *handler) addHandlerContext(messages ...*Message) {
	for i, msg := range messages {
		ctx := msg.Context()

		if h.name != "" {
			ctx = context.WithValue(ctx, handlerNameKey, h.name)
		}
		if h.publisherName != "" {
			ctx = context.WithValue(ctx, publisherNameKey, h.publisherName)
		}
		if h.subscriberName != "" {
			ctx = context.WithValue(ctx, subscriberNameKey, h.subscriberName)
		}
		if h.subscribeTopic != "" {
			ctx = context.WithValue(ctx, subscribeTopicKey, h.subscribeTopic)
		}
		if h.publishTopic != "" {
			ctx = context.WithValue(ctx, publishTopicKey, h.publishTopic)
		}
		messages[i].SetContext(ctx)
	}
}

func (h *handler) handleClose(ctx context.Context) {
	select {
	case <-h.routersCloseCh:
		// for backward compatibility we are closing subscriber
		h.logger.Debug("Waiting for subscriber to close", nil)
		if err := h.subscriber.Close(); err != nil {
			h.logger.Error("Failed to close subscriber", err, nil)
		}
		h.logger.Debug("Subscriber closed", nil)
	case <-ctx.Done():
		// we are closing subscriber just when entire router is closed
	}
	h.stopFn()
}

func (h *handler) handleMessage(msg *Message, handler HandlerFunc) {
	defer h.runningHandlersWg.Done()
	msgFields := watermill.LogFields{"message_uuid": msg.UUID}

	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Error(
				"Panic recovered in handler. Stack: "+string(debug.Stack()),
				errors.Errorf("%s", recovered),
				msgFields,
			)
			msg.Nack()
		}
	}()

	h.logger.Trace("Received message", msgFields)

	producedMessages, err := handler(msg)
	if err != nil {
		h.logger.Error("Handler returned error", err, nil)
		msg.Nack()
		return
	}

	h.addHandlerContext(producedMessages...)

	if err := h.publishProducedMessages(producedMessages, msgFields); err != nil {
		h.logger.Error("Publishing produced messages failed", err, nil)
		msg.Nack()
		return
	}

	msg.Ack()
	h.logger.Trace("Message acked", msgFields)
}

func (h *handler) publishProducedMessages(producedMessages Messages, msgFields watermill.LogFields) error {
	if len(producedMessages) == 0 {
		return nil
	}

	if h.publisher == nil {
		return ErrOutputInNoPublisherHandler
	}

	h.logger.Trace("Sending produced messages", msgFields.Add(watermill.LogFields{
		"produced_messages_count": len(producedMessages),
		"publish_topic":           h.publishTopic,
	}))

	for _, msg := range producedMessages {
		if err := h.publisher.Publish(h.publishTopic, msg); err != nil {
			// todo - how to deal with it better/transactional/retry?
			h.logger.Error("Cannot publish message", err, msgFields.Add(watermill.LogFields{
				"not_sent_message": fmt.Sprintf("%#v", producedMessages),
			}))

			return err
		}
	}

	return nil
}

type disabledPublisher struct{}

func (disabledPublisher) Publish(topic string, messages ...*Message) error {
	return ErrOutputInNoPublisherHandler
}

func (disabledPublisher) Close() error {
	return nil
}
