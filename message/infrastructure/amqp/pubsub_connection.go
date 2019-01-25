package amqp

import (
	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func (p *PubSub) connect() error {
	p.connectionLock.Lock()
	defer p.connectionLock.Unlock()

	if p.config.AmqpConfig != nil && p.config.AmqpConfig.TLSClientConfig != nil && p.config.TLSConfig != nil {
		return errors.New("both Config.AmqpConfig.TLSClientConfig and Config.TLSConfig are set")
	}

	var connection *amqp.Connection
	var err error

	if p.config.AmqpConfig != nil {
		connection, err = amqp.DialConfig(p.config.AmqpURI, *p.config.AmqpConfig)
	} else if p.config.TLSConfig != nil {
		connection, err = amqp.DialTLS(p.config.AmqpURI, p.config.TLSConfig)
	} else {
		connection, err = amqp.Dial(p.config.AmqpURI)
	}

	if err != nil {
		return errors.Wrap(err, "cannot connect to AMQP")
	}
	p.connection = connection
	close(p.connected)

	p.logger.Info("Connected to AMQP", nil)

	return nil
}

func (p *PubSub) Connection() *amqp.Connection {
	return p.connection
}

func (p *PubSub) Connected() chan struct{} {
	return p.connected
}

func (p *PubSub) IsConnected() bool {
	select {
	case <-p.connected:
		return true
	default:
		return false
	}
}

func (p *PubSub) handleConnectionClose() {
	for {
		p.logger.Debug("handleConnectionClose is waiting for p.connected", nil)
		<-p.connected
		p.logger.Debug("handleConnectionClose is for connection or Pub/Sub close", nil)

		notifyCloseConnection := p.connection.NotifyClose(make(chan *amqp.Error))

		select {
		case <-p.closing:
			p.logger.Debug("Stopping handleConnectionClose", nil)
			return
		case err := <-notifyCloseConnection:
			p.connected = make(chan struct{})
			p.logger.Error("Received close notification from AMQP, reconnecting", err, nil)
			p.reconnect()
		}
	}
}

func (p *PubSub) reconnect() {
	if err := backoff.Retry(func() error {
		err := p.connect()
		if err == nil {
			return nil
		}

		p.logger.Error("Cannot reconnect to AMQP, retrying", err, nil)

		if p.closed {
			return backoff.Permanent(errors.Wrap(err, "closing AMQP PubSub"))
		}

		return err
	}, p.config.Reconnect.backoffConfig()); err != nil {
		// should only exit, if closing Pub/Sub
		p.logger.Error("AMQP reconnect failed failed", err, nil)
	}
}
