package main

func NewAccountFromHistory(id string, events []Event) *Account {
	a := NewAccount(id)

	for _, e := range events {
		a.update(e)
		a.PopNewEvents() // todo - test and move to stdlib
	}

	return a
}

func (a *Account) update(event Event) {
	switch v := event.(type) {
	case Withdrawed:
		a.handleWithdrawed(v)
	case Deposited:
		a.handleDeposited(v)
	default:
		panic("event not supported") // todo - panic?
	}

	a.RecordThat(event)
}
