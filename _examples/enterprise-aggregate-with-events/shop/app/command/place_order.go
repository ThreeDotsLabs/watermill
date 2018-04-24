package command

import (
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/cart"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/order"
	"github.com/roblaszczak/gooddd/domain"
)

type PlaceOrder struct {
	OrderID    order.ID
	CartID     cart.ID
	CustomerID string // todo - better type
}

type PlaceOrderHandler struct {
	cartRepo      cart.Repository
	eventstore    domain.Eventstore
	eventsFactory domain.EventsFactory
}

func NewPlaceOrderHandler(cartRepo cart.Repository, eventstore domain.Eventstore, eventsFactory domain.EventsFactory) PlaceOrderHandler {
	return PlaceOrderHandler{cartRepo, eventstore, eventsFactory}
}

func (h PlaceOrderHandler) Handle(cmd PlaceOrder) error {
	ordersService := order.NewService()

	cart, err := h.cartRepo.ByID(cmd.CartID)
	if err != nil {
		return errors.Wrapf(err, "cannot get cart %s", cmd.CartID)
	}

	if err := ordersService.PlaceOrder(cmd.OrderID, cmd.CartID, cmd.CustomerID, cart.Products()); err != nil {
		return errors.Wrap(err, "cannot place orde")
	}

	// todo -simplify it!
	h.eventstore.Save(h.eventsFactory.NewEvents(ordersService.PopEvents()))

	return nil
}
