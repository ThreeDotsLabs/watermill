package amqp

import (
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/ThreeDotsLabs/watermill"
)

type connectionWrapper struct {
	config Config

	logger watermill.LoggerAdapter

	amqpConnection     *amqp.Connection
	amqpConnectionLock sync.Mutex
	connected          chan struct{}

	publishBindingsLock     sync.RWMutex
	publishBindingsPrepared map[string]struct{}

	closing chan struct{}
	closed  bool

	publishingWg  sync.WaitGroup
	subscribingWg sync.WaitGroup
}

func newConnection(
	config Config,
	logger watermill.LoggerAdapter,
) (*connectionWrapper, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	pubSub := &connectionWrapper{
		config:    config,
		logger:    logger,
		closing:   make(chan struct{}),
		connected: make(chan struct{}),
	}
	if err := pubSub.connect(); err != nil {
		return nil, err
	}

	go pubSub.handleConnectionClose()

	return pubSub, nil
}

func (c *connectionWrapper) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.closing)

	c.logger.Info("Closing AMQP Pub/Sub", nil)
	defer c.logger.Info("Closed AMQP Pub/Sub", nil)

	c.publishingWg.Wait()

	if err := c.amqpConnection.Close(); err != nil {
		c.logger.Error("Connection close error", err, nil)
	}

	c.subscribingWg.Wait()

	return nil
}

func (c *connectionWrapper) connect() error {
	c.amqpConnectionLock.Lock()
	defer c.amqpConnectionLock.Unlock()

	amqpConfig := c.config.Connection.AmqpConfig
	if amqpConfig != nil && amqpConfig.TLSClientConfig != nil && c.config.Connection.TLSConfig != nil {
		return errors.New("both Config.AmqpConfig.TLSClientConfig and Config.TLSConfig are set")
	}

	var connection *amqp.Connection
	var err error

	if amqpConfig != nil {
		connection, err = amqp.DialConfig(c.config.Connection.AmqpURI, *c.config.Connection.AmqpConfig)
	} else if c.config.Connection.TLSConfig != nil {
		connection, err = amqp.DialTLS(c.config.Connection.AmqpURI, c.config.Connection.TLSConfig)
	} else {
		connection, err = amqp.Dial(c.config.Connection.AmqpURI)
	}

	if err != nil {
		return errors.Wrap(err, "cannot connect to AMQP")
	}
	c.amqpConnection = connection
	close(c.connected)

	c.logger.Info("Connected to AMQP", nil)

	return nil
}

func (c *connectionWrapper) Connection() *amqp.Connection {
	return c.amqpConnection
}

func (c *connectionWrapper) Connected() chan struct{} {
	return c.connected
}

func (c *connectionWrapper) IsConnected() bool {
	select {
	case <-c.connected:
		return true
	default:
		return false
	}
}

func (c *connectionWrapper) handleConnectionClose() {
	for {
		c.logger.Debug("handleConnectionClose is waiting for p.connected", nil)
		<-c.connected
		c.logger.Debug("handleConnectionClose is for connection or Pub/Sub close", nil)

		notifyCloseConnection := c.amqpConnection.NotifyClose(make(chan *amqp.Error))

		select {
		case <-c.closing:
			c.logger.Debug("Stopping handleConnectionClose", nil)
			return
		case err := <-notifyCloseConnection:
			c.connected = make(chan struct{})
			c.logger.Error("Received close notification from AMQP, reconnecting", err, nil)
			c.reconnect()
		}
	}
}

func (c *connectionWrapper) reconnect() {
	reconnectConfig := c.config.Connection.Reconnect
	if reconnectConfig == nil {
		reconnectConfig = DefaultReconnectConfig()
	}

	if err := backoff.Retry(func() error {
		err := c.connect()
		if err == nil {
			return nil
		}

		c.logger.Error("Cannot reconnect to AMQP, retrying", err, nil)

		if c.closed {
			return backoff.Permanent(errors.Wrap(err, "closing AMQP connection"))
		}

		return err
	}, reconnectConfig.backoffConfig()); err != nil {
		// should only exit, if closing Pub/Sub
		c.logger.Error("AMQP reconnect failed failed", err, nil)
	}
}
