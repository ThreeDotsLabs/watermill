package message

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"sync"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal"
	sync_internal "github.com/ThreeDotsLabs/watermill/pubsub/sync"
)

// RouterPlugin is function which is executed on Router start.
type DynRouterPlugin func(*DynRouter) error

// RouteMessageFunc is function called when message is received.
type RouteMessageFunc func(topic string, msg []*Message) (string, []*Message, error)

// NewDynRouter creates a new Router with given configuration.
func NewDynRouter(config RouterConfig, routeMsgFunc RouteMessageFunc, logger watermill.LoggerAdapter) (*DynRouter, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	return &DynRouter{
		config: config,

		handlers:     map[string]*routeMsgHandler{},
		routeMsgFunc: routeMsgFunc,

		handlersWg:        &sync.WaitGroup{},
		runningHandlersWg: &sync.WaitGroup{},
		handlerAdded:      make(chan struct{}),

		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),

		logger: logger,

		running: make(chan struct{}),
	}, nil
}

// DynRouter is responsible for handling messages from subscribers using provided handler functions.
//
// If the handler function returns a message, the message is published with the publisher.
// You can use middlewares to wrap handlers with common logic like logging, instrumentation, etc.
type DynRouter struct {
	config RouterConfig

	routeMsgFunc RouteMessageFunc

	middlewares []middleware

	plugins []DynRouterPlugin

	handlers     map[string]*routeMsgHandler
	handlersLock sync.RWMutex

	handlersWg        *sync.WaitGroup
	runningHandlersWg *sync.WaitGroup
	handlerAdded      chan struct{}

	closeCh    chan struct{}
	closedCh   chan struct{}
	closed     bool
	closedLock sync.Mutex

	logger watermill.LoggerAdapter

	publisherDecorators  []PublisherDecorator
	subscriberDecorators []SubscriberDecorator

	isRunning bool
	running   chan struct{}
}

// Logger returns the Router's logger.
func (r *DynRouter) Logger() watermill.LoggerAdapter {
	return r.logger
}

// RouteMsgHandler handles Messages.
type RouteMsgHandler struct {
	router  *DynRouter
	handler *routeMsgHandler
}

// AddMiddleware adds a new middleware to the router.
//
// The order of middleware matters. Middleware added at the beginning is executed first.
func (r *DynRouter) AddMiddleware(m ...HandlerMiddleware) {
	r.logger.Debug("Adding middleware", watermill.LogFields{"count": fmt.Sprintf("%d", len(m))})

	r.addRouterLevelMiddleware(m...)
}

func (r *DynRouter) addRouterLevelMiddleware(m ...HandlerMiddleware) {
	for _, handlerMiddleware := range m {
		middleware := middleware{
			Handler:       handlerMiddleware,
			HandlerName:   "",
			IsRouterLevel: true,
		}
		r.middlewares = append(r.middlewares, middleware)
	}
}

func (r *DynRouter) addHandlerLevelMiddleware(handlerName string, m ...HandlerMiddleware) {
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
func (r *DynRouter) AddPlugin(p ...DynRouterPlugin) {
	r.logger.Debug("Adding plugins", watermill.LogFields{"count": fmt.Sprintf("%d", len(p))})

	r.plugins = append(r.plugins, p...)
}

// AddPublisherDecorators wraps the router's Publisher.
// The first decorator is the innermost, i.e. calls the original publisher.
func (r *DynRouter) AddPublisherDecorators(dec ...PublisherDecorator) {
	r.logger.Debug("Adding publisher decorators", watermill.LogFields{"count": fmt.Sprintf("%d", len(dec))})

	r.publisherDecorators = append(r.publisherDecorators, dec...)
}

// AddSubscriberDecorators wraps the router's Subscriber.
// The first decorator is the innermost, i.e. calls the original subscriber.
func (r *DynRouter) AddSubscriberDecorators(dec ...SubscriberDecorator) {
	r.logger.Debug("Adding subscriber decorators", watermill.LogFields{"count": fmt.Sprintf("%d", len(dec))})

	r.subscriberDecorators = append(r.subscriberDecorators, dec...)
}

// Handlers returns all registered handlers.
func (r *DynRouter) Handlers() map[string]HandlerFunc {
	handlers := map[string]HandlerFunc{}

	for handlerName, handler := range r.handlers {
		handlers[handlerName] = handler.handlerFunc
	}

	return handlers
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
func (r *DynRouter) AddHandler(
	handlerName string,
	subscribeTopic string,
	subscriber Subscriber,
	publishTopic string,
	publisher Publisher,
	handlerFunc HandlerFunc,
) *RouteMsgHandler {
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

	newHandler := &routeMsgHandler{
		name:   handlerName,
		logger: r.logger,

		subscriber:     subscriber,
		subscribeTopic: subscribeTopic,
		subscriberName: subscriberName,

		publisher:     publisher,
		publishTopic:  publishTopic,
		publisherName: publisherName,

		handlerFunc:       handlerFunc,
		runningHandlersWg: r.runningHandlersWg,
		messagesCh:        nil,
		routersCloseCh:    r.closeCh,

		startedCh: make(chan struct{}),
	}

	r.handlersWg.Add(1)
	r.handlers[handlerName] = newHandler

	select {
	case r.handlerAdded <- struct{}{}:
	default:
		// closeWhenAllHandlersStopped is not always waiting for handlerAdded
	}

	return &RouteMsgHandler{
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
func (r *DynRouter) AddNoPublisherHandler(
	handlerName string,
	subscribeTopic string,
	subscriber Subscriber,
	handlerFunc NoPublishHandlerFunc,
) *RouteMsgHandler {
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
func (r *DynRouter) Run(ctx context.Context) (err error) {
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

	go r.closeWhenAllHandlersStopped()

	<-r.closeCh
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
func (r *DynRouter) RunHandlers(ctx context.Context) error {
	if !r.isRunning {
		return errors.New("you can't call RunHandlers on non-running router")
	}

	r.handlersLock.Lock()
	defer r.handlersLock.Unlock()

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

			h.runRM(ctx, r.middlewares, r.routeMsgFunc)

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
func (r *DynRouter) closeWhenAllHandlersStopped() {
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
	if r.isClosed() {
		// already closed
		return
	}

	r.logger.Error("All handlers stopped, closing router", errors.New("all router handlers stopped"), nil)

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
func (r *DynRouter) Running() chan struct{} {
	return r.running
}

// IsRunning returns true when router is running.
func (r *DynRouter) IsRunning() bool {
	select {
	case <-r.running:
		return true
	default:
		return false
	}
}

// Close gracefully closes the router with a timeout provided in the configuration.
func (r *DynRouter) Close() error {
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

	close(r.closeCh)
	defer close(r.closedCh)

	timeouted := sync_internal.WaitGroupTimeout(r.handlersWg, r.config.CloseTimeout)
	if timeouted {
		return errors.New("router close timeout")
	}

	return nil
}

func (r *DynRouter) isClosed() bool {
	r.closedLock.Lock()
	defer r.closedLock.Unlock()

	return r.closed
}

type routeMsgHandler struct {
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

func (h *routeMsgHandler) runRM(ctx context.Context, middlewares []middleware, routeMsgFunc RouteMessageFunc) {
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

	go h.handleRMClose(ctx)

	for msg := range h.messagesCh {
		h.runningHandlersWg.Add(1)
		h.handleRMMessage(msg, routeMsgFunc, middlewareHandler)
		// go h.handleMessage(msg, handleMessageFunc, middlewareHandler)
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

func (h *routeMsgHandler) handleMessage(msg *Message, routeMsgFunc RouteMessageFunc, handler HandlerFunc) {
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

	h.logger.Trace("Received message fields", msgFields)

	producedMessages, err := handler(msg)
	if err != nil {
		h.logger.Error("Handler returned error", err, nil)
		msg.Nack()
		return
	}

	// TODO:
	fmt.Printf("before topic: %v\n", h.publishTopic)
	if isJSON(string(msg.Payload)) {
		var m *Message
		err := json.Unmarshal(msg.Payload, &m)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("before producedMessages: %v\n", m)
	} else {
		fmt.Printf("before producedMessages: %v\n", msg.Payload)
	}
	returnedTopic, producedMessages, err := routeMsgFunc(h.publishTopic, producedMessages)
	fmt.Printf("after topic: %v\n", h.publishTopic)
	fmt.Printf("after producedMessages: %v\n", producedMessages)

	h.addRMHandlerContext(producedMessages...)
	var targetTopic string
	if returnedTopic != "" {
		targetTopic = returnedTopic
	} else {
		targetTopic = h.publishTopic
	}

	if err := h.publishProducedRMMessages(targetTopic, producedMessages, msgFields); err != nil {
		h.logger.Error("Publishing produced messages failed", err, nil)
		msg.Nack()
		return
	}

	msg.Ack()
	h.logger.Trace("Message acked", msgFields)
}

// addHandlerContextRm enriches the contex with values that are relevant within this handler's context.
func (h *routeMsgHandler) addHandlerContextRm(messages ...*Message) {
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

func (h *routeMsgHandler) handleRMClose(ctx context.Context) {
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

// decorateHandlerPublisher applies the decorator chain to handler's publisher.
// They are applied in reverse order, so that the later decorators use the result of former ones.
func (r *DynRouter) decorateHandlerPublisher(h *routeMsgHandler) error {
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
func (r *DynRouter) decorateHandlerSubscriber(h *routeMsgHandler) error {
	var err error
	sub := h.subscriber

	// add values to message context to subscriber
	// it goes before other decorators, so that they may take advantage of these values
	messageTransform := func(msg *Message) {
		if msg != nil {
			h.addRMHandlerContext(msg)
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

// addRMHandlerContext enriches the contex with values that are relevant within this handler's context.
func (h *routeMsgHandler) addRMHandlerContext(messages ...*Message) {
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

func (h *routeMsgHandler) handleRMMessage(msg *Message, routeMsgFunc RouteMessageFunc, handler HandlerFunc) {
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

	h.logger.Trace("Received message fields", msgFields)

	producedMessages, err := handler(msg)
	if err != nil {
		h.logger.Error("Handler returned error", err, nil)
		msg.Nack()
		return
	}

	// TODO:
	fmt.Printf("before topic: %v\n", h.publishTopic)
	if isJSON(string(msg.Payload)) {
		var m *Message
		err := json.Unmarshal(msg.Payload, &m)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("before producedMessages: %v\n", m)
	} else {
		fmt.Printf("before producedMessages: %v\n", msg.Payload)
	}
	returnedTopic, producedMessages, err := routeMsgFunc(h.publishTopic, producedMessages)
	fmt.Printf("after topic: %v\n", h.publishTopic)
	fmt.Printf("after producedMessages: %v\n", producedMessages)

	h.addRMHandlerContext(producedMessages...)
	var targetTopic string
	if returnedTopic != "" {
		targetTopic = returnedTopic
	} else {
		targetTopic = h.publishTopic
	}

	if err := h.publishProducedRMMessages(targetTopic, producedMessages, msgFields); err != nil {
		h.logger.Error("Publishing produced messages failed", err, nil)
		msg.Nack()
		return
	}

	msg.Ack()
	h.logger.Trace("Message acked", msgFields)
}

func isJSON(text string) bool {
	var js map[string]interface{}

	// Attempt to unmarshal the JSON
	err := json.Unmarshal([]byte(text), &js)
	return err == nil
}

func (h *routeMsgHandler) publishProducedRMMessages(targetTopic string, producedMessages Messages, msgFields watermill.LogFields) error {
	if len(producedMessages) == 0 {
		return nil
	}

	if h.publisher == nil {
		return ErrOutputInNoPublisherHandler
	}

	h.logger.Trace("Sending produced messages", msgFields.Add(watermill.LogFields{
		"produced_messages_count": len(producedMessages),
		"publish_topic":           targetTopic,
	}))

	for _, msg := range producedMessages {
		if err := h.publisher.Publish(targetTopic, msg); err != nil {
			// todo - how to deal with it better/transactional/retry?
			h.logger.Error("Cannot publish message", err, msgFields.Add(watermill.LogFields{
				"not_sent_message": fmt.Sprintf("%#v", producedMessages),
			}))

			return err
		}
	}

	return nil
}
