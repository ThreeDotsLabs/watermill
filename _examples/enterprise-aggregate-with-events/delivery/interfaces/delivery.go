package interfaces

import (
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/delivery/app"
	"github.com/roblaszczak/gooddd/pubsub"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/shop/domain/order"
	"github.com/roblaszczak/gooddd/domain"
	"github.com/roblaszczak/gooddd/msghandler"
	"github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/delivery/infrastructure/address"
	order2 "github.com/roblaszczak/gooddd/_examples/enterprise-aggregate-with-events/delivery/infrastructure/order"
)

// todo - rename
func SetupInterfaces(r *msghandler.Router) {
	// todo - move it out
	//r.AddMiddleware(middleware.AckMiddleware)

	// todo - is bind good world? subscribe? bind? handle? todo - look for it
	r.Subscribe(
		"test",
		// todo - use shared kernel for events? todo - make some  magic func? method on message?
		DeliveryEventsHandler{app.NewDeliveryService(address.ServiceStub{}, order2.ServiceStub{})}.Handle,
		//DeliveryEventsHandler{}.Handle,
	)
}

// todo - DeliveryMessagesHandler
type DeliveryEventsHandler struct {
	deliveryService app.DeliveryService
}

func (h DeliveryEventsHandler) Handle(msg pubsub.Message) ([]pubsub.MessagePayload, error) {
	// todo - user bounded context type? (too much data + copuling?)
	orderPlaced, ok := msg.Payload().(order.Placed)
	if !ok {
		// not supported event, just ignoring
		// todo -  middleware?
		return nil, nil
	}

	if err := h.deliveryService.InitDelivery(string(orderPlaced.OrderID), orderPlaced.CustomerID); err != nil {
		return nil, err
	}

	// todo - a bit ugly?
	return domain.EventsToMessagePayloads(h.deliveryService.PopEvents()), nil
}
