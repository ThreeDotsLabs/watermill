package events

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type Publisher struct {
	EventBus *cqrs.EventBus
}

func (p Publisher) Publish(event interface{}) error {
	return p.EventBus.Publish(context.Background(), event)
}
