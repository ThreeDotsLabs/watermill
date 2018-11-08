package cart

import (
	"testing"

	cartDomain "github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	cartInfra "github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/infrastructurecture/cart"
	"github.com/stretchr/testify/assert"
)

// todo - snake case?

func TestRepository(t *testing.T) {
	r := cartInfra.NewMemoryRepository()

	_, err := r.ByID("1")
	assert.Equal(t, cartDomain.ErrNotFound, err)

	c := cartDomain.NewCart("1")
	err = r.Save(c)
	assert.NoError(t, err)

	repoCart, err := r.ByID("1")
	assert.NoError(t, err)

	assert.EqualValues(t, c, repoCart)
}
