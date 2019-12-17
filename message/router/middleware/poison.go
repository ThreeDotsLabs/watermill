package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// ErrInvalidPoisonQueueTopic occurs when the topic supplied to the PoisonQueue constructor is invalid.
var ErrInvalidPoisonQueueTopic = errors.New("invalid poison queue topic")

// ReasonForPoisonedKey is the metadata key which marks the reason (error) why the message was deemed poisoned.
var ReasonForPoisonedKey = "reason_poisoned"
var PoisonedTopicKey = "topic_poisoned"
var PoisonedHandlerKey = "handler_poisoned"
var PoisonedSubscriberKey = "subscriber_poisoned"

type poisonQueue struct {
	topic string
	pub   message.Publisher

	shouldGoToPoisonQueue func(err error) bool
}

// PoisonQueue provides a middleware that salvages unprocessable messages and published them on a separate topic.
// The main middleware chain then continues on, business as usual.
func PoisonQueue(pub message.Publisher, topic string) (message.HandlerMiddleware, error) {
	if topic == "" {
		return nil, ErrInvalidPoisonQueueTopic
	}

	pq := poisonQueue{
		topic: topic,
		pub:   pub,
		shouldGoToPoisonQueue: func(err error) bool {
			return true
		},
	}

	return pq.Middleware, nil
}

// PoisonQueueWithFilter is just like PoisonQueue, but accepts a function that decides which errors qualify for the poison queue.
func PoisonQueueWithFilter(pub message.Publisher, topic string, shouldGoToPoisonQueue func(err error) bool) (message.HandlerMiddleware, error) {
	if topic == "" {
		return nil, ErrInvalidPoisonQueueTopic
	}

	pq := poisonQueue{
		topic: topic,
		pub:   pub,

		shouldGoToPoisonQueue: shouldGoToPoisonQueue,
	}

	return pq.Middleware, nil
}

func (pq poisonQueue) publishPoisonMessage(msg *message.Message, err error) error {
	// no problems encountered, carry on
	if err == nil {
		return nil
	}

	// add context why it was poisoned
	msg.Metadata.Set(ReasonForPoisonedKey, err.Error())
	msg.Metadata.Set(PoisonedTopicKey, message.SubscribeTopicFromCtx(msg.Context()))
	msg.Metadata.Set(PoisonedHandlerKey, message.HandlerNameFromCtx(msg.Context()))
	msg.Metadata.Set(PoisonedSubscriberKey, message.SubscriberNameFromCtx(msg.Context()))

	// don't intercept error from publish. Can't help you if the publisher is down as well.
	return pq.pub.Publish(pq.topic, msg)
}

func (pq poisonQueue) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (events []*message.Message, err error) {
		defer func() {
			if err != nil {
				if !pq.shouldGoToPoisonQueue(err) {
					return
				}

				// handler didn't cope with the message; publish it on the poison topic and carry on as usual
				publishErr := pq.publishPoisonMessage(msg, err)
				if publishErr != nil {
					publishErr = errors.Wrap(publishErr, "cannot publish message to poison queue")
					err = multierror.Append(err, publishErr)
					return
				}

				err = nil
				return
			}
		}()

		// if h fails, the deferred function will salvage all that it can
		return h(msg)
	}
}
