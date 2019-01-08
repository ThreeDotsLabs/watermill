package http

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/pkg/errors"
)

var (
	ErrNonPositiveNumberOfRetries  = errors.New("number of retries should be positive")
	ErrNonPositiveTimeToFirstRetry = errors.New("time to first retry should be positive")
)

type RetryPublisherConfig struct {
	MaxRetries int
	// each subsequent retry doubles the time to next retry.
	TimeToFirstRetry time.Duration
}

func (c *RetryPublisherConfig) setDefaults() {
	if c.MaxRetries == 0 {
		c.MaxRetries = 5
	}

	if c.TimeToFirstRetry == 0 {
		c.TimeToFirstRetry = time.Second
	}
}

func (c RetryPublisherConfig) validate() error {
	if c.MaxRetries <= 0 {
		return ErrNonPositiveNumberOfRetries
	}
	if c.TimeToFirstRetry <= 0 {
		return ErrNonPositiveTimeToFirstRetry
	}

	return nil
}

type RetryPublisher struct {
	pub    *Publisher
	config RetryPublisherConfig
}

func NewRetryPublisher(pub *Publisher, config RetryPublisherConfig) (*RetryPublisher, error) {
	config.setDefaults()

	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid RetryPublisher config")
	}

	return &RetryPublisher{
		pub,
		config,
	}, nil
}

func (p RetryPublisher) Publish(topic string, messages ...*message.Message) error {
	var err error
	timeToNextRetry := p.config.TimeToFirstRetry

	for i := 0; i < p.config.MaxRetries; i++ {
		err = p.pub.Publish(topic, messages...)
		if err == nil {
			return nil
		}

		p.pub.logger.Debug("publish failed, retrying in "+timeToNextRetry.String(), watermill.LogFields{})
		time.Sleep(timeToNextRetry)
		timeToNextRetry *= 2
	}

	return err
}

func (p RetryPublisher) Close() error {
	return p.pub.Close()
}
