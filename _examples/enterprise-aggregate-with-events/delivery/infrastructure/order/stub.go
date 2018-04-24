package order

import "fmt"

type ServiceStub struct{}

func (s ServiceStub) OrderDelivery(orderID string, address string) error {
	fmt.Printf("Sending order %s to %s", orderID, address)

	return nil
}
