package publisher_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/internal/publisher"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

var ErrCouldNotPublish = errors.New("could not publish, try again")

// FailingPublisher mocks a publisher that fails N times, then succeeds.
type FailingPublisher struct {
	howManyFails int
}

func (p *FailingPublisher) Publish(topic string, messages ...*message.Message) error {
	if p.howManyFails > 0 {
		p.howManyFails--
		return ErrCouldNotPublish
	}

	return nil
}

func (p *FailingPublisher) Close() error {
	return nil
}

func TestRetryPublisher_Publish_after_retries(t *testing.T) {
	pub := FailingPublisher{howManyFails: 4}
	conf := publisher.RetryPublisherConfig{
		MaxRetries:       5,
		TimeToFirstRetry: time.Millisecond,
		Logger:           watermill.NopLogger{},
	}
	retryPub, err := publisher.NewRetryPublisher(&pub, conf)
	require.NoError(t, err)

	// given
	require.True(t, pub.howManyFails < conf.MaxRetries, "Publisher must fail less than MaxRetriesTimes")

	// when
	err = retryPub.Publish("topic", message.NewMessage("uuid", []byte{}))

	// then
	require.NoError(t, err)
}

func TestRetryPublisher_Publish_too_many_retries(t *testing.T) {
	pub := FailingPublisher{howManyFails: 5}
	conf := publisher.RetryPublisherConfig{
		MaxRetries:       5,
		TimeToFirstRetry: time.Millisecond,
		Logger:           watermill.NopLogger{},
	}
	retryPub, err := publisher.NewRetryPublisher(&pub, conf)
	require.NoError(t, err)

	// given
	require.True(t, pub.howManyFails >= conf.MaxRetries, "Publisher must fail at least MaxRetries times")

	// when
	err = retryPub.Publish("topic", message.NewMessage("uuid", []byte{}))

	// then
	require.Error(t, err)
	assert.Equal(t, ErrCouldNotPublish, errors.Cause(err))
}

var ErrCouldNotClose = errors.New("this publisher fails on Close()")

// ClosingPublisher mocks a publisher that may fail on closing so we may check if the Close() call propagated correctly.
type ClosingPublisher struct {
	closed      bool
	failOnClose bool
}

func (ClosingPublisher) Publish(topic string, messages ...*message.Message) error {
	return nil
}

func (p *ClosingPublisher) Close() error {
	if p.failOnClose {
		return ErrCouldNotClose
	}
	p.closed = true
	return nil
}

func TestRetryPublisher_Close(t *testing.T) {
	pub := ClosingPublisher{}
	retryPub, err := publisher.NewRetryPublisher(&pub, publisher.RetryPublisherConfig{})
	require.NoError(t, err)

	// given
	require.False(t, pub.closed)

	// when
	err = retryPub.Close()

	// then
	require.NoError(t, err)
	assert.True(t, pub.closed)
}

func TestRetryPublisher_Close_failed(t *testing.T) {
	pub := ClosingPublisher{failOnClose: true}
	retryPub, err := publisher.NewRetryPublisher(&pub, publisher.RetryPublisherConfig{})
	require.NoError(t, err)

	// given
	require.False(t, pub.closed)

	// when
	err = retryPub.Close()

	// then
	require.Error(t, err)
	assert.Equal(t, ErrCouldNotClose, errors.Cause(err))
}
