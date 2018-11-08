package middleware_test

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

type mockPublisherBehaviour int

const (
	BehaviourAlwaysOK mockPublisherBehaviour = iota + 1
	BehaviourAlwaysFail
	BehaviourAlwaysPanic
)

var (
	errClosed   = errors.New("closed")
	errFailed   = errors.New("failed")
	errPanicked = errors.New("panicked")
)

type mockPublisher struct {
	behaviour mockPublisherBehaviour
	closed    bool

	produced []*message.Message
}

func (mp *mockPublisher) Publish(topic string, message *message.Message) error {
	if mp.closed {
		return errClosed
	}

	switch mp.behaviour {
	case BehaviourAlwaysOK:
	case BehaviourAlwaysFail:
		return errFailed
	case BehaviourAlwaysPanic:
		panic(errPanicked)
	}

	mp.produced = append(mp.produced, message)
	return nil
}

func (mp *mockPublisher) ClosePublisher() error {
	mp.closed = true
	return nil
}

func (mp *mockPublisher) PopMessages() []*message.Message {
	defer func() { mp.produced = []*message.Message{} }()
	return mp.produced
}

var handlerFuncAlwaysOKMessages = []*message.Message{
	message.NewMessage(uuid.NewV4().String(), nil),
	message.NewMessage(uuid.NewV4().String(), nil),
}

func handlerFuncAlwaysOK(*message.Message) ([]*message.Message, error) {
	return handlerFuncAlwaysOKMessages, nil
}

func handlerFuncAlwaysFailing(*message.Message) ([]*message.Message, error) {
	return nil, errFailed
}
