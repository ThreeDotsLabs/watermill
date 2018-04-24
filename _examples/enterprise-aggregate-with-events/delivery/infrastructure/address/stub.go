package address

type ServiceStub struct {}


func (s ServiceStub) CustomerAddress(customerID string) (string, error) {
	return "3828 Piermont Dr, Albuquerque, New Mexico 87112 USA", nil
}
