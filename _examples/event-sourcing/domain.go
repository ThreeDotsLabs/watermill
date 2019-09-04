package main

// Inspiration: https://github.com/microsoftarchive/cqrs-journey/blob/6ffd9a8c8e865a9f8209552c52fa793fbd496d1f/source/Infrastructure/Sql/Infrastructure.Sql/EventSourcing/SqlEventSourcedRepository.cs
// Inspiration: https://github.com/microsoftarchive/cqrs-journey/blob/6ffd9a8c8e865a9f8209552c52fa793fbd496d1f/source/Infrastructure/Infrastructure/EventSourcing/EventSourced.cs
// Insporation: https://github.com/microsoftarchive/cqrs-journey/blob/6ffd9a8c8e865a9f8209552c52fa793fbd496d1f/source/Infrastructure/Sql/Infrastructure.Sql/EventSourcing/EventStoreDbContext.cs

import (
	"errors"
)

type Withdrawed struct {
	Amount int
}

type Deposited struct {
	Amount int
}

// todo - just idea
//go:generate watermill-es generate -aggregate=Account -idGetter=ID -constructor=NewAccount

type Account struct {
	EventProducer // todo - how to make it private?

	id      string
	balance int
}

// todo - should have id in param?
func NewAccount(id string) *Account {
	return &Account{id: id}
}

func (a *Account) ID() string {
	return a.id
}

func (a *Account) Balance() int {
	return a.balance
}

func (a *Account) Withdraw(amount int) error {
	if a.balance < amount {
		return errors.New("not enough money")
	}

	// todo - how to avoid calling handleWithdrawed instead of update?
	a.update(Withdrawed{amount})
	return nil
}

func (a *Account) handleWithdrawed(w Withdrawed) {
	a.balance -= w.Amount
}

func (a *Account) Deposit(amount int) {
	a.update(Deposited{amount})
}

func (a *Account) handleDeposited(d Deposited) {
	a.balance = d.Amount
}
