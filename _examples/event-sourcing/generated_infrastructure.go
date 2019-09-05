package main

import (
	"errors"
)

type AccountEventSourcedRepository struct {
	events map[string][]Event
}

func (r *AccountEventSourcedRepository) Save(account *Account) error {
	key := account.ID()

	if _, ok := r.events[key]; !ok {
		r.events[key] = []Event{}
	}

	r.events[key] = append(r.events[key], account.PopChanges()...)

	return nil
}

func (r AccountEventSourcedRepository) Find(id string) (*Account, error) {
	events, err := r.findEvents(id)
	if err != nil {
		return nil, err
	}

	return NewAccountFromHistory(events), nil
}

func (r AccountEventSourcedRepository) findEvents(id string) ([]Event, error) {
	events, ok := r.events[id]
	if !ok {
		return nil, errors.New("not found") //todo - better err
	}

	return events, nil
}
