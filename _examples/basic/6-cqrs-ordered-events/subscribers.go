package main

import (
	"context"
	"log/slog"
	"sync"
)

type SubscriberReadModel struct {
	subscribers map[string]string // map[subscriberID]email
	lock        sync.RWMutex
}

func NewSubscriberReadModel() *SubscriberReadModel {
	return &SubscriberReadModel{
		subscribers: make(map[string]string),
	}
}

func (m *SubscriberReadModel) OnSubscribed(ctx context.Context, event *SubscriberSubscribed) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.subscribers[event.SubscriberId] = event.Email

	slog.Info(
		"Subscriber added",
		"subscriber_id", event.SubscriberId,
		"email", event.Email,
	)

	return nil
}

func (m *SubscriberReadModel) OnUnsubscribed(ctx context.Context, event *SubscriberUnsubscribed) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.subscribers, event.SubscriberId)

	slog.Info(
		"Subscriber removed",
		"subscriber_id", event.SubscriberId,
	)

	return nil
}

func (m *SubscriberReadModel) OnEmailUpdated(ctx context.Context, event *SubscriberEmailUpdated) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.subscribers[event.SubscriberId] = event.NewEmail

	slog.Info(
		"Subscriber updated",
		"subscriber_id", event.SubscriberId,
		"email", event.NewEmail,
	)

	return nil
}

func (m *SubscriberReadModel) GetSubscriberCount() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.subscribers)
}
