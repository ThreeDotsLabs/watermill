package saga

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type Saga interface {
	ID() string                   // todo - []bytes? interface{}?
	IsComplete() bool             // todo - return error?
	OnComplete() *message.Message // todo - return error?
	HandleMessage(*message.Message)
}

type Persister interface {
	Upsert(id string) (Saga, error)
	Persist(Saga) error
}

type GenerateSagaID func(*message.Message) string

// todo test
type Handler struct {
	Costam    func(*message.Message, Persister) Saga
	persister Persister
}

func (h Handler) Handler(consumedMessage *message.Message) ([]*message.Message, error) {
	saga, err := h.persister.Upsert()
	if err != nil {
		return nil, errors.Wrap(err, "saga: cannot upsert saga")
	}

	if saga.IsComplete() {
		// todo - test
		return nil, nil
	}

	if err := saga.HandleMessage(consumedMessage); err != nil {
		return nil, errors.Wrap(err, "saga: message handing failed")
	}

	var messages []*message.Message
	if saga.IsComplete() {
		messages = []*message.Message{saga.OnComplete()}
	}

	if err := h.persister.Persist(saga); err != nil {
		return nil, errors.Wrap(err, "saga: cannot persist saga")
	}

	return messages, nil
}
