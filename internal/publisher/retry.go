package publisher

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
	Logger           watermill.LoggerAdapter
}

func (c *RetryPublisherConfig) setDefaults() {
	if c.MaxRetries == 0 {
		c.MaxRetries = 5
	}

	if c.TimeToFirstRetry == 0 {
		c.TimeToFirstRetry = time.Second
	}

	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
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

// RetryPublisher is a decorator for a publisher that retries message publishing after a failure.
type RetryPublisher struct {
	pub    message.Publisher
	config RetryPublisherConfig
}

func NewRetryPublisher(pub message.Publisher, config RetryPublisherConfig) (*RetryPublisher, error) {
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
	failedMessages := NewErrCouldNotPublish()

	// todo: do some parallel processing maybe? this is a very basic implementation
	for _, msg := range messages {
		err := p.send(topic, msg)
		if err != nil {
			failedMessages.addMsg(msg, err)
		}
	}

	if failedMessages.Len() > 0 {
		return failedMessages
	}

	return nil
}

func (p RetryPublisher) Close() error {
	return p.pub.Close()
}

// send sends one message at a time to prevent sending a successful message more than once.
func (p RetryPublisher) send(topic string, msg *message.Message) error {
	var err error
	timeToNextRetry := p.config.TimeToFirstRetry

	for i := 0; i < p.config.MaxRetries; i++ {
		err = p.pub.Publish(topic, msg)
		if err == nil {
			return nil
		}

		p.config.Logger.Info("Publish failed, retrying in "+timeToNextRetry.String(), watermill.LogFields{
			"error": err,
		})
		time.Sleep(timeToNextRetry)
		timeToNextRetry *= 2
	}
	return err
}
