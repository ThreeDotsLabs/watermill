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

	generateTopic         func(messageType string) string
	subscriberConstructor MessageSubscriberConstructor

	logger watermill.LoggerAdapter
}

func NewMessageBus(
	router *message.Router,
	generateTopic func(messageType string) string,
	subscriberConstructor MessageSubscriberConstructor,
	logger watermill.LoggerAdapter,
) (*MessageBus, error) {
	if router == nil {
		return nil, errors.New("missing router")
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
		generateTopic,
		subscriberConstructor,
		logger,
	}, nil
}

func (b *MessageBus) AddHandler(handler MessageHandler) error {
	topicName := b.generateTopic(handler.MessageType)

	logger := b.logger.With(watermill.LogFields{
		"handler_name": handler.Name,
		"messageType":  handler.MessageType,
		"topic":        topicName,
	})

	logger.Debug("Adding bus message handler to router", nil)

	subscriber, err := b.subscriberConstructor(handler.Name)
	if err != nil {
		return errors.Wrap(err, "cannot create subscriber for message bus")
	}

	b.router.AddNoPublisherHandler(
		handler.Name,
		topicName,
		subscriber,
		handler.Handler,
	)

	return nil
}

type MessageHandler struct {
	Name        string
	MessageType string
	Handler     message.NoPublishHandlerFunc
}

func NewMessageHandler[M any](
	handlerName string,
	handlerFunc func(ctx context.Context, message M) error,
	marshaler MessageMarshaler,
	logger watermill.LoggerAdapter,
) MessageHandler {
	expectedType := marshaler.Type(new(M))

	handler := func(msg *message.Message) error {
		messageStruct := new(M)
		messageType := marshaler.TypeFromMessage(msg)

		if messageType != expectedType {
			logger.Trace("Received different message type than expected, ignoring", watermill.LogFields{
				"message_uuid":          msg.UUID,
				"expected_message_type": expectedType,
				"received_message_type": messageType,
			})
			return nil
		}

		logger.Debug("Handling message", watermill.LogFields{
			"message_uuid":          msg.UUID,
			"received_message_type": messageType,
		})

		if err := marshaler.Unmarshal(msg, messageStruct); err != nil {
			return err
		}

		if err := handlerFunc(msg.Context(), *messageStruct); err != nil {
			// TODO consider this to be logged as Error?
			logger.Debug("Error when handling message", watermill.LogFields{"err": err})
			return err
		}

		return nil
	}

	return MessageHandler{
		Name:        handlerName,
		MessageType: expectedType,
		Handler:     handler,
	}
}
