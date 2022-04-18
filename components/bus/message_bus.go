package bus

import (
	"context"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type MessageSubscriberConstructor func(handlerName string) (message.Subscriber, error)

type MessageBus struct {
	router *message.Router

	marshaler             MessageMarshaler
	generateTopic         func(messageType string) string
	subscriberConstructor MessageSubscriberConstructor

	logger watermill.LoggerAdapter
}

func NewMessageBus(
	router *message.Router,
	marshaler MessageMarshaler,
	generateTopic func(messageType string) string,
	subscriberConstructor MessageSubscriberConstructor,
	logger watermill.LoggerAdapter,
) (*MessageBus, error) {
	if router == nil {
		return nil, errors.New("missing router")
	}
	if marshaler == nil {
		return nil, errors.New("missing marshaler")
	}
	if generateTopic == nil {
		return nil, errors.New("missing generateTopic")
	}
	if subscriberConstructor == nil {
		return nil, errors.New("missing subscriberConstructor")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &MessageBus{
		router,
		marshaler,
		generateTopic,
		subscriberConstructor,
		logger,
	}, nil
}

func AddHandler[M any](
	b *MessageBus,
	handlerName string,
	handlerFunc func(ctx context.Context, message M) error,
) error {
	messageType := b.marshaler.Type(new(M))
	topicName := b.generateTopic(messageType)

	logger := b.logger.With(watermill.LogFields{
		"handler_name": handlerName,
		"messageType":  messageType,
		"topic":        topicName,
	})

	logger.Debug("Adding bus message handler to router", nil)

	subscriber, err := b.subscriberConstructor(handlerName)
	if err != nil {
		return errors.Wrap(err, "cannot create subscriber for message bus")
	}

	b.router.AddNoPublisherHandler(
		handlerName,
		topicName,
		subscriber,
		newMessageHandler(b, handlerFunc),
	)

	return nil
}

func newMessageHandler[M any](b *MessageBus, handlerFunc func(ctx context.Context, message M) error) message.NoPublishHandlerFunc {
	return func(msg *message.Message) error {
		expectedType := b.marshaler.Type(new(M))
		messageStruct := new(M)
		messageType := b.marshaler.TypeFromMessage(msg)

		if messageType != expectedType {
			b.logger.Trace("Received different message type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_message_type": expectedType,
				"received_message_type": messageType,
			})
			return nil
		}

		b.logger.Debug("Handling message", watermill.LogFields{
			"message_uuid":          msg.UUID,
			"received_message_type": messageType,
		})

		if err := b.marshaler.Unmarshal(msg, messageStruct); err != nil {
			return err
		}

		if err := handlerFunc(msg.Context(), *messageStruct); err != nil {
			// TODO consider this to be logged as Error?
			b.logger.Debug("Error when handling message", watermill.LogFields{"err": err})
			return err
		}

		return nil
	}
}
