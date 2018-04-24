package order

import (
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/product"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/domain"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/cart"
)

var (
	ErrEmptyID         = errors.New("empty order ID")
	ErrEmptyCartID     = errors.New("empty cart ID")
	ErrEmptyCustomerID = errors.New("empty customer ID")
	ErrEmptyProducts   = errors.New("missing products ID's")
	ErrEmptyProductID  = errors.New("empty product ID")
)

type ID string

// todo doc- domain service - sending events from service
type Service struct {
	// todo - use events bus?
	*domain.EventProducer
}

func NewService() *Service {
	return &Service{&domain.EventProducer{}}
}

func (s Service) PlaceOrder(id ID, cartID cart.ID, customerID string, products []product.ID) (error) {
	if id == "" {
		return ErrEmptyID
	}

	if cartID == "" {
		return ErrEmptyCartID
	}

	if len(products) == 0 {
		return ErrEmptyProducts
	}

	if customerID == "" {
		return ErrEmptyCustomerID
	}

	for _, p := range products {
		if p == "" {
			return errors.Wrapf(ErrEmptyProductID, "ids: %s", products)
		}
	}

	s.RecordThat(Placed{baseEvent{id, 0}, cartID, customerID, products})

	return nil
}
