package common

import (
	"encoding/json"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const UpdatesTopic = "updates"

func NotifyMiddleware(pub message.Publisher, serviceName string) func(message.HandlerFunc) message.HandlerFunc {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			topic := message.SubscribeTopicFromCtx(msg.Context())
			handler := strings.Split(message.HandlerNameFromCtx(msg.Context()), "-")[0]

			msgs, err := next(msg)
			if err != nil {
				return msgs, err
			}

			payload := MessageReceived{
				ID:      msg.UUID,
				Service: serviceName,
				Handler: handler,
				Topic:   topic,
			}

			jsonPayload, err := json.Marshal(payload)
			if err != nil {
				return nil, err
			}

			newMsg := message.NewMessage(watermill.NewUUID(), jsonPayload)

			err = pub.Publish(UpdatesTopic, newMsg)
			if err != nil {
				return nil, err
			}

			return msgs, nil
		}
	}
}
