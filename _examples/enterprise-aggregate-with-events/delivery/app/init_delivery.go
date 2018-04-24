package app

// todo - doc that we are using only app

import (
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/domain"
	"github.com/satori/go.uuid"
)

type addressService interface {
	CustomerAddress(customerID string) (string, error)
}

type deliveryProvider interface {
	OrderDelivery(orderID string, address string) error
}

type DeliveryService struct {
	addressService   addressService
	deliveryProvider deliveryProvider

	*domain.EventProducer
}

func NewDeliveryService(addressService addressService, deliveryProvider deliveryProvider) DeliveryService {
	return DeliveryService{addressService, deliveryProvider, &domain.EventProducer{}}
}

type DeliveryInitialized struct {
	DeliveryID string
	OrderID    string
	CustomerID string
}

func (e DeliveryInitialized) AggregateID() string {
	return e.DeliveryID
}

func (e DeliveryInitialized) AggregateType() string {
	return "delivery"
}

func (e DeliveryInitialized) AggregateVersion() int {
	return 0
}

func (d DeliveryService) InitDelivery(orderID string, customerID string) error {
	addr, err := d.addressService.CustomerAddress(customerID)
	if err != nil {
		return errors.Wrapf(err, "cannot get address of %s", customerID)
	}

	if err := d.deliveryProvider.OrderDelivery(orderID, addr); err != nil {
		return errors.Wrapf(err, "cannot order delivery of order %s", orderID)
	}

	d.RecordThat(DeliveryInitialized{uuid.NewV1().String(), orderID, customerID}) // todo - change aggregate type

	return nil
}
