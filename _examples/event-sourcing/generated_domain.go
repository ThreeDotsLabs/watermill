package main

func NewAccountFromHistory(events []Event) *Account {
	a := &Account{}

	for _, e := range events {
		a.update(e)
		a.PopChanges() // todo - test and move to stdlib
	}

	return a
}

func (a *Account) recordThat(event Event) {
	if event == nil {
		return
	}
	a.es.changes = append(a.es.changes, event)
	a.es.version += 1
}

// todo - test without pitor
// todo - does we need pop?
func (a *Account) PopChanges() []Event {
	defer func() { a.es.changes = nil }()
	return a.es.changes
}

func (a *Account) update(event Event) {
	switch v := event.(type) {
	case AccountCreated:
		a.handleAccountCreated(v)
	case Withdrawed:
		a.handleWithdrawed(v)
	case Deposited:
		a.handleDeposited(v)
	default:
		panic("event not supported") // todo - panic?
	}

	a.recordThat(event)
}

func (a *Account) Version() int64 {
	return a.es.version
}
