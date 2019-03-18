package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// ErrInvalidPoisonQueueTopic occurs when the topic supplied to the PoisonQueue constructor is invalid.
var ErrInvalidPoisonQueueTopic = errors.New("invalid poison queue topic")

// PoisonQueue provides a middleware that salvages unprocessable messages and published them on a separate topic.
// The main middleware chain then continues on, business as usual.
type PoisonQueue struct {
	topic string
	pub   message.Publisher

	shouldGoToPoisonQueue func(err error) bool
}

// ReasonForPoisonedKey is the metadata key which marks the reason (error) why the message was deemed poisoned.
var ReasonForPoisonedKey = "reason_poisoned"

func (pq PoisonQueue) publishPoisonMessage(msg *message.Message, err error) error {
	// no problems encountered, carry on
	if err == nil {
		return nil
	}

	// add context why it was poisoned
	msg.Metadata.Set(ReasonForPoisonedKey, err.Error())

	// don't intercept error from publish. Can't help you if the publisher is down as well.
	return pq.pub.Publish(pq.topic, msg)
}

func NewPoisonQueue(pub message.Publisher, topic string) (PoisonQueue, error) {
	if topic == "" {
		return PoisonQueue{}, ErrInvalidPoisonQueueTopic
	}

	pq := PoisonQueue{
		topic: topic,
		pub:   pub,
		shouldGoToPoisonQueue: func(err error) bool {
			return true
		},
	}

	return pq, nil
}

// todo - rename?
func NewPoisonQueueWithFilter(pub message.Publisher, topic string, shouldGoToPoisonQueue func(err error) bool) (PoisonQueue, error) {
	if topic == "" {
		return PoisonQueue{}, ErrInvalidPoisonQueueTopic
	}

	pq := PoisonQueue{
		topic: topic,
		pub:   pub,

		shouldGoToPoisonQueue: shouldGoToPoisonQueue,
	}

	return pq, nil
}

func (pq PoisonQueue) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (events []*message.Message, err error) {
		defer func() {
			if err != nil {
				if !pq.shouldGoToPoisonQueue(err) {
					return
				}

				// handler didn't cope with the message; publish it on the poison topic and carry on as usual
				publishErr := pq.publishPoisonMessage(msg, err)
				if err != nil {
					publishErr = errors.Wrap(publishErr, "cannot publish message to poison queue")
					err = multierror.Append(err, publishErr)
				}
			}
		}()

		// if h fails, the deferred function will salvage all that it can
		return h(msg)
	}
}
