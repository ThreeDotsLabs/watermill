package middleware

import (
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

// ErrInvalidPoisonQueueTopic occurs when the topic supplied to the PoisonQueue constructor is invalid.
var ErrInvalidPoisonQueueTopic = errors.New("invalid poison queue topic")

// PoisonQueue provides a middleware that salvages unprocessable messages and published them on a separate topic.
// The main middleware chain then continues on, business as usual.
type PoisonQueue struct {
	topic      string
	pub        message.Publisher
	Middleware message.HandlerMiddleware
}

// ReasonForPoisonedKey is the metadata key which marks the reason (error) why the message was deemed poisoned.
var ReasonForPoisonedKey = "reason_poisoned"

func (pq PoisonQueue) publishPoisonMessage(msg message.ConsumedMessage, err error) (error) {
	// no problems encountered, carry on
	if err == nil {
		return  nil
	}

	// republish the original message on the poisoned topic
	var payload message.Payload
	msg.UnmarshalPayload(&payload)
	producedMsg := message.NewDefault(msg.UUID(), payload)

	// add context why it was poisoned
	producedMsg.SetMetadata(ReasonForPoisonedKey, err.Error())

	// don't intercept error from publish. Can't help you if the publisher is down as well.
	return pq.pub.Publish(pq.topic, []message.ProducedMessage{producedMsg})
}

func NewPoisonQueue(pub message.Publisher, topic string) (PoisonQueue, error) {
	if topic == "" {
		return PoisonQueue{}, ErrInvalidPoisonQueueTopic
	}

	pq := PoisonQueue{
		topic: topic,
		pub:   pub,
	}

	pq.Middleware = func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg message.ConsumedMessage) (events []message.ProducedMessage, err error) {
			defer func() {
				r := recover()
				if r != nil {
					var ok bool
					err, ok = r.(error)
					if !ok {
						err = errors.Errorf("recovered panic with %+v", err)
					}
				}

				if err != nil {
					// handler didn't cope with the message; publish it on the poison topic and carry on as usual
					err = pq.publishPoisonMessage(msg, err)
					return
				}
			}()

			// if h fails, the deferred function will salvage all that it can
			return h(msg)
		}
	}
	return pq, nil
}
