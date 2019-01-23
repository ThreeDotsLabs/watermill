package amqp

import (
	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

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

func (p *PubSub) isConnected() bool {
	select {
	case <-p.connected:
		return true
	default:
		return false
	}
}

func (p *PubSub) connect() error {
	p.connectionLock.Lock()
	defer p.connectionLock.Unlock()

	// todo - support for tls
	connection, err := amqp.Dial(p.config.AmqpURI)
	if err != nil {
		return errors.Wrap(err, "cannot connect to AMQP")
	}
	p.connection = connection
	close(p.connected)

	p.logger.Info("Connected to AMQP", nil)

	return nil
}
