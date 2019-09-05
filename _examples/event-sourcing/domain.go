package main

// Inspiration: https://github.com/microsoftarchive/cqrs-journey/blob/6ffd9a8c8e865a9f8209552c52fa793fbd496d1f/source/Infrastructure/Sql/Infrastructure.Sql/EventSourcing/SqlEventSourcedRepository.cs
// Inspiration: https://github.com/microsoftarchive/cqrs-journey/blob/6ffd9a8c8e865a9f8209552c52fa793fbd496d1f/source/Infrastructure/Infrastructure/EventSourcing/EventSourced.cs
// Inspiration: https://github.com/microsoftarchive/cqrs-journey/blob/6ffd9a8c8e865a9f8209552c52fa793fbd496d1f/source/Infrastructure/Sql/Infrastructure.Sql/EventSourcing/EventStoreDbContext.cs
// Inspiration: https://github.com/jen20/go-event-sourcing-sample

import (
	"errors"
)

type AccountCreated struct {
	UUID string
}

type Withdrawed struct {
	Amount int
}

type Deposited struct {
	Amount int
}

// todo - just idea
//go:generate watermill-es generate -aggregate=Account -idGetter=UUID

type Account struct {
	es eventSourced // todo - how to make it private?

	uuid    string
	balance int
}

func CreateNewAccount(uuid string) *Account {
	a := &Account{}
	a.update(AccountCreated{uuid})

	return a
}

func (a *Account) ID() string {
	return a.uuid
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

func (a *Account) handleAccountCreated(created AccountCreated) {
	a.uuid = created.UUID
}
