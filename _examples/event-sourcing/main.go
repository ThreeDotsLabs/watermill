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

func (e *EventProducer) RecordThat(event Event) {
	if event == nil {
		return
	}
	e.events = append(e.events, event)
}

// todo - test without pitor
func (e *EventProducer) PopEvents() []Event {
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

	id      string
	balance int
}

// todo - should have id in param?
func NewAccount(id string) *Account {
	return &Account{id: id}
}

func (a Account) AggregateType() string {
	return "account"
}

func NewAccountFromHistory(id string, events []Event) *Account {
	a := &Account{id: id}

	for _, e := range events {
		a.update(e)
		a.PopEvents() // todo - test and move to stdlib
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

	a.RecordThat(event) // todo - move out of update? to standard lib?
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
	a1 := NewAccount("1")
	a1.Deposit(10)
	if err := a1.Withdraw(3); err != nil {
		panic(err)
	}

	fmt.Println(a1.Balance())

	a2 := NewAccountFromHistory("2", []Event{Deposited{15}, Withdrawed{3}})
	fmt.Println(a2.Balance())

	repo := &AccountRepository{
		repo: BaseEventSourcedRepository{
			events: make(map[string][]Event),
		},
	}
	if err := repo.Save(a2); err != nil {
		panic(err)
	}

	a2Repo, err := repo.Find(a2.ID())
	if err != nil {
		panic(err)
	}

	fmt.Println("repo", a2Repo.Balance())
}

type EventSourced interface {
	ID() string
	AggregateType() string
	PopEvents() []Event
}

type AccountRepository struct {
	repo BaseEventSourcedRepository
}

func (r *AccountRepository) Save(account *Account) error {
	return r.repo.Save(account)
}

func (r AccountRepository) Find(id string) (*Account, error) {
	events, err := r.repo.FindEvents(id)
	if err != nil {
		return nil, err
	}

	return NewAccountFromHistory(id, events), nil
}

// todo - rename
type BaseEventSourcedRepository struct {
	events map[string][]Event
}

func (r *BaseEventSourcedRepository) Save(aggregate EventSourced) error {
	key := string(aggregate.ID())

	if _, ok := r.events[key]; !ok {
		r.events[key] = []Event{}
	}

	r.events[key] = append(r.events[key], aggregate.PopEvents()...)

	return nil
}

func (r BaseEventSourcedRepository) FindEvents(id string) ([]Event, error) {
	fmt.Println(r.events)

	events, ok := r.events[id]
	if !ok {
		return nil, errors.New("not found") //todo - better err
	}

	return events, nil
}
