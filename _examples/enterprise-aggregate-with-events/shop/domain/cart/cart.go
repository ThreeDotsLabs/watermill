package cart

import (
	"github.com/roblaszczak/gooddd/domain"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/product"
)

type ID string

type Cart struct {
	*domain.EventProducer // todo - easy to forget about pitor :(

	id ID

	products []product.ID

	version int
}

// todo - doc - sending events from aggregate
func NewCart(id ID) *Cart {
	// todo - validate id

	c := &Cart{EventProducer: &domain.EventProducer{}}
	c.id = id

	c.RecordThat(Created{c.newBaseEvent()})

	return c
}

func (c *Cart) newBaseEvent() baseEvent {
	defer func() { c.version++ }()
	return baseEvent{c.id, c.version}
}

// todo - test
func (c *Cart) Version() int {
	return c.version
}

func (c *Cart) ID() ID {
	return c.id
}

func (c *Cart) PutProduct(productID product.ID) {
	c.products = append(c.products, productID)
	c.RecordThat(ProductPut{c.newBaseEvent(), productID}) // todo - use some kind of constructor?
}

func (c *Cart) Products() []product.ID {
	return c.products
}
