package cart

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/product"
	"github.com/ThreeDotsLabs/watermill/components/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewCart(t *testing.T) {
	c := cart.NewCart("1")

	assert.Equal(t, cart.ID("1"), c.ID())
	assert.Equal(t, []domain.Event{cart.Created{"1"}}, c.PopEvents())
}

func TestCart_InsertProduct(t *testing.T) {
	c := cart.NewCart("cart_1")
	// clean up events
	c.PopEvents()

	c.PutProduct("product_1")

	assert.Equal(t, []product.ID{"product_1"}, c.Products())
	assert.Equal(t, []domain.Event{cart.ProductPut{"cart_1", "product_1"}}, c.PopEvents())
}
