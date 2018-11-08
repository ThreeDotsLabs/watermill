package order

import (
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/product"
)

type baseEvent struct {
	OrderID ID
	version int
}

func (e baseEvent) AggregateID() string {
	return string(e.OrderID)
}

func (e baseEvent) AggregateType() string {
	return "order"
}

func (e baseEvent) AggregateVersion() int {
	return e.version
}

type Placed struct {
	baseEvent
	CartID     cart.ID
	CustomerID string // todo - better id?
	Products   []product.ID
}
