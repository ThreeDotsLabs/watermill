package order

import (
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/product"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/cart"
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
