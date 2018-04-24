package cart

import "github.com/pkg/errors"

var (
	ErrNotFound = errors.New("cart not found")
)

type Repository interface {
	Save(cart *Cart) error
	ByID(id ID) (*Cart, error)
}
