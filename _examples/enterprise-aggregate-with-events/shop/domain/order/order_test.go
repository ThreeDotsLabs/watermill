package order

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/order"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/product"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOrder(t *testing.T) {
	testCases := []struct {
		Name        string
		ID          order.ID
		CartID      cart.ID
		CustomerID  string
		Products    []product.ID
		ExpectedErr error
	}{
		{
			Name:       "valid",
			ID:         "order_1",
			CartID:     "cart_1",
			CustomerID: "customer_1",
			Products:   []product.ID{"product_1"},
		},
		{
			Name:        "empty_id",
			ID:          "",
			CartID:      "cart_1",
			CustomerID:  "customer_1",
			Products:    []product.ID{"product_1"},
			ExpectedErr: order.ErrEmptyID,
		},
		{
			Name:        "empty_products",
			ID:          "order_1",
			CartID:      "cart_1",
			CustomerID:  "customer_1",
			Products:    []product.ID{},
			ExpectedErr: order.ErrEmptyProducts,
		},
		{
			Name:        "empty_product_id",
			ID:          "order_1",
			CartID:      "cart_1",
			CustomerID:  "customer_1",
			Products:    []product.ID{"product_1", ""},
			ExpectedErr: order.ErrEmptyProductID,
		},
		{
			Name:        "valid",
			ID:          "order_1",
			CartID:      "",
			CustomerID:  "customer_1",
			Products:    []product.ID{"product_1"},
			ExpectedErr: order.ErrEmptyCartID,
		},
		{
			Name:       "valid",
			ID:         "order_1",
			CartID:     "cart_1",
			CustomerID: "customer_1",
			Products:   []product.ID{"product_1"},
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			s := order.NewService()
			err := s.PlaceOrder(c.ID, c.CartID, c.CustomerID, c.Products)

			if c.ExpectedErr == nil {
				events := s.PopEvents()
				require.Len(t, events, 1)
				placedEvent := events[0].(order.Placed)

				assert.NoError(t, err)
				assert.Equal(t, c.ID, placedEvent.OrderID)
				assert.Equal(t, c.Products, placedEvent.Products)
				assert.Equal(t, c.CartID, placedEvent.CartID)
			} else {
				assert.Equal(t, c.ExpectedErr, errors.Cause(err))
			}
		})
	}
}
