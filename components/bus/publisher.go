package bus

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type Publisher struct {
	publisher     message.Publisher
	generateTopic func(messageType string) string
	marshaler     MessageMarshaler
}

func NewPublisher(
	publisher message.Publisher,
	generateTopic func(messageType string) string,
	marshaler MessageMarshaler,
) (*Publisher, error) {
	if publisher == nil {
		return nil, errors.New("missing publisher")
	}
	if generateTopic == nil {
		return nil, errors.New("missing generateTopic")
	}
	if marshaler == nil {
		return nil, errors.New("missing marshaler")
	}

	return &Publisher{publisher, generateTopic, marshaler}, nil
}

func (p Publisher) Publish(ctx context.Context, message any) error {
	msg, err := p.marshaler.Marshal(ctx, message)
	if err != nil {
		return err
	}

	messageType := p.marshaler.Type(message)
	topicName := p.generateTopic(messageType)

	msg.SetContext(ctx)

	return p.publisher.Publish(topicName, msg)
}
