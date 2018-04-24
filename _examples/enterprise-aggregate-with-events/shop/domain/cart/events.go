package cart

import (
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/product"
)

type baseEvent struct {
	CartID ID
	version int
}

func (e baseEvent) AggregateID() string {
	return string(e.CartID)
}

func (e baseEvent) AggregateType() string {
	return "cart"
}

func (e baseEvent) AggregateVersion() int {
	return e.version
}

type Created struct {
	baseEvent
}

type ProductPut struct {
	baseEvent
	ProductID product.ID
}
