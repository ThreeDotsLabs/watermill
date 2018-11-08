package command

import (
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	"github.com/ThreeDotsLabs/watermill/_examples/enterprise-aggregate-with-events/shop/domain/product"
	"github.com/ThreeDotsLabs/watermill/components/domain"
	"github.com/pkg/errors"
)

type PutProductToCart struct {
	CartID    cart.ID
	ProductID product.ID
}

type PutProductToCartHandler struct {
	repo          cart.Repository
	eventstore    domain.Eventstore
	eventsFactory domain.EventsFactory
}

func NewPutProductToCartHandler(
	repo cart.Repository,
	eventstore domain.Eventstore,
	eventsFactory domain.EventsFactory,
) PutProductToCartHandler {
	return PutProductToCartHandler{repo, eventstore, eventsFactory}
}

func (h PutProductToCartHandler) Handle(cmd PutProductToCart) error {
	c, err := h.repo.ByID(cmd.CartID)
	if err != nil {
		return errors.Wrapf(err, "cannot get cart %s", cmd.CartID)
	}

	c.PutProduct(cmd.ProductID)

	if err := h.repo.Save(c); err != nil {
		return errors.Wrap(err, "cannot save cart")
	}

	h.eventstore.Save(h.eventsFactory.NewEvents(c.PopEvents()))

	return nil
}
