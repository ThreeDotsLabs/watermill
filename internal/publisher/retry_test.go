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

var errCouldNotPublish = errors.New("could not publish, try again")

// FailingPublisher mocks a publisher that fails a specific number of time for a message, then succeeds.
type FailingPublisher struct {
	howManyFails     map[string]int
	howManyPublished map[string]int
}

func (p *FailingPublisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		howManyFails, ok := p.howManyFails[msg.UUID]
		if !ok || howManyFails <= 0 {
			p.publish(msg)
			continue
		}
		p.howManyFails[msg.UUID]--
		return errCouldNotPublish
	}

	return nil
}

func (p *FailingPublisher) publish(msg *message.Message) {
	if _, ok := p.howManyPublished[msg.UUID]; !ok {
		p.howManyPublished[msg.UUID] = 1
		return
	}
	p.howManyPublished[msg.UUID]++
}

func (p *FailingPublisher) Close() error {
	return nil
}

func TestRetryPublisher_Publish_after_retries(t *testing.T) {
	msg := message.NewMessage("uuid", []byte{})
	pub := FailingPublisher{
		howManyFails: map[string]int{
			msg.UUID: 4,
		},
		howManyPublished: map[string]int{},
	}
	conf := publisher.RetryPublisherConfig{
		MaxRetries:       5,
		TimeToFirstRetry: time.Millisecond,
		Logger:           watermill.NopLogger{},
	}
	retryPub, err := publisher.NewRetryPublisher(&pub, conf)
	require.NoError(t, err)

	// given
	require.True(t, pub.howManyFails[msg.UUID] < conf.MaxRetries, "Publisher must fail less than MaxRetries times")

	// when
	err = retryPub.Publish("topic", msg)

	// then
	require.NoError(t, err)
	assert.Contains(t, pub.howManyPublished, msg.UUID)
	assert.Equal(t, pub.howManyPublished[msg.UUID], 1, "Expected msg to be published exactly once")
}

func TestRetryPublisher_Publish_too_many_retries(t *testing.T) {
	msg := message.NewMessage("uuid", []byte{})
	pub := FailingPublisher{
		howManyFails: map[string]int{
			msg.UUID: 5,
		},
		howManyPublished: map[string]int{},
	}
	conf := publisher.RetryPublisherConfig{
		MaxRetries:       5,
		TimeToFirstRetry: time.Millisecond,
		Logger:           watermill.NopLogger{},
	}
	retryPub, err := publisher.NewRetryPublisher(&pub, conf)
	require.NoError(t, err)

	// given
	require.True(t, pub.howManyFails[msg.UUID] >= conf.MaxRetries, "Publisher must fail at least MaxRetries times")

	// when
	err = retryPub.Publish("topic", msg)

	// then
	require.Error(t, err)

	cnpErr, ok := err.(*publisher.ErrCouldNotPublish)
	require.True(t, ok, "expected the ErrCouldNotPublish composite error type")

	assert.Equal(t, 1, cnpErr.Len(), "attempted to publish one message, expecting one error")
	assert.Equal(t, errCouldNotPublish, errors.Cause(cnpErr.Reasons()[msg.UUID]))

	assert.NotContains(t, pub.howManyPublished, msg.UUID, "expected msg to not be published")
}

func TestPublishEachMessageOnlyOnce(t *testing.T) {
	msg1 := message.NewMessage("uuid1", []byte{})
	msg2 := message.NewMessage("uuid2", []byte{})

	pub := FailingPublisher{
		howManyFails: map[string]int{
			msg1.UUID: 2,
			msg2.UUID: 4,
		},
		howManyPublished: map[string]int{},
	}
	conf := publisher.RetryPublisherConfig{
		MaxRetries:       5,
		TimeToFirstRetry: time.Millisecond,
		Logger:           watermill.NopLogger{},
	}
	retryPub, err := publisher.NewRetryPublisher(&pub, conf)
	require.NoError(t, err)

	// given
	require.True(t, pub.howManyFails[msg1.UUID] < conf.MaxRetries, "Publisher must fail less than MaxRetries times for msg1")
	require.True(t, pub.howManyFails[msg2.UUID] < conf.MaxRetries, "Publisher must fail less than MaxRetries times for msg2")
	require.True(t, pub.howManyFails[msg1.UUID] < pub.howManyFails[msg2.UUID], "Publisher must fail less times for msg1 than msg2")

	// when
	err = retryPub.Publish("topic", msg1, msg2)

	// then
	require.NoError(t, err)

	assert.Equal(t, pub.howManyPublished[msg1.UUID], 1, "expected msg1 to be published only once")
	assert.Equal(t, pub.howManyPublished[msg2.UUID], 1, "expected msg2 to be published only once")
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
