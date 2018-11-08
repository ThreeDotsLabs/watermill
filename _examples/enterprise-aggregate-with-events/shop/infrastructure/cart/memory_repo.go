package cart

import "github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/cart"

type MemoryRepository struct {
	carts map[cart.ID]cart.Cart
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{map[cart.ID]cart.Cart{}}
}

func (m *MemoryRepository) Save(cart *cart.Cart) error {
	// todo - saving with pitor, good?
	m.carts[cart.ID()] = *cart
	return nil
}

func (m *MemoryRepository) ByID(id cart.ID) (*cart.Cart, error) {
	for _, c := range m.carts {
		if c.ID() == id {
			return &c, nil
		}
	}

	return nil, cart.ErrNotFound
}
