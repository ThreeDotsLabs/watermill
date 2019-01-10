package main

import (
	"fmt"

	"github.com/pkg/errors"
)

type Event interface {
	EventType() string
}

// todo - imlementation detail? not for watermill?
type EventTransprt struct {
	EventID          []byte
	AggregateID      []byte
	AggregateType    string
	AggregateVersion int
	EventPayload     []byte
}

type EventProducer struct {
	events []Event
}

func (e EventProducer) RecordThat(event Event) {
	if event == nil {
		return
	}
	e.events = append(e.events, event)
}

// todo - test without pitor
func (e EventProducer) PopEvents() []Event {
	defer func() { e.events = nil }()
	return e.events
}

type Withdrawed struct {
	Amount int
}

func (Withdrawed) EventType() string {
	return "Withdrawed"
}

type Deposited struct {
	Amount int
}

func (Deposited) EventType() string {
	return "Deposited"
}

type Account struct {
	EventProducer

	id      []byte
	balance int
}

func NewAccountFromHistory(events []Event) *Account {
	a := &Account{}
	for _, e := range events {
		a.update(e)
	}

	return a
}

func (a *Account) update(event Event) {
	switch v := event.(type) {
	case Withdrawed:
		a.updateWithdrawed(v)
	case Deposited:
		a.updateDeposited(v)
	default:
		panic("event not supported") // todo - panic?
	}

	a.RecordThat(event)
}

func (a *Account) ID() []byte {
	return a.id
}

func (a *Account) Balance() int {
	return a.balance
}

func (a *Account) Withdraw(amount int) error {
	if a.balance < amount {
		return errors.New("not enough money")
	}

	a.update(Withdrawed{amount})
	return nil
}

func (a *Account) updateWithdrawed(w Withdrawed) {
	a.balance -= w.Amount
}

func (a *Account) Deposit(amount int) {
	a.update(Deposited{amount})
}

func (a *Account) updateDeposited(d Deposited) {
	a.balance = d.Amount
}

func main() {
	a1 := Account{}
	a1.Deposit(10)
	if err := a1.Withdraw(3); err != nil {
		panic(err)
	}

	fmt.Println(a1.Balance())

	a2 := NewAccountFromHistory([]Event{Deposited{15}, Withdrawed{3}})
	fmt.Println(a2.Balance())
}

type EventSourced interface {
	ID() []byte
	AggregateType() string
	PopEvents() []Event
}

type AccountRepository struct {
	repo BaseEventSourcedRepository
}

func (r AccountRepository) Save(account *Account) error {

}

func (r AccountRepository) Find(id []byte) (*Account, error) {

}

// todo - rename
type BaseEventSourcedRepository struct {
	events map[string][]Event
}

func (r BaseEventSourcedRepository) Save(aggregate EventSourced) error {
	key := string(aggregate.ID())

	if _, ok := r.events[key]; !ok {
		r.events[key] = []Event{}
	}

	r.events[key] = append(r.events[key], aggregate.PopEvents()...)
}

func (r BaseEventSourcedRepository) Find() {

}
