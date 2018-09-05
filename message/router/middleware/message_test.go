package middleware_test

import (
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

type mockProducedMessage struct {
	metadata map[string]string
	payload  interface{}
}

func NewMockProducedMessage(payload interface{}) message.ProducedMessage {
	return &mockProducedMessage{
		metadata: map[string]string{
			"key1": "metadata_value",
			"key2": "metadata_value",
		},
		payload: payload,
	}
}

func (mockProducedMessage) UUID() string {
	return "uuid"
}

func (mpm mockProducedMessage) GetMetadata(key string) string {
	return mpm.metadata[key]
}

func (mpm mockProducedMessage) AllMetadata() map[string]string {
	return mpm.metadata
}

func (mpm *mockProducedMessage) SetMetadata(key, value string) {
	mpm.metadata[key] = value
}

func (mpm mockProducedMessage) Payload() message.Payload {
	return mpm.payload
}

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

	produced []message.ProducedMessage
}

func (mp *mockPublisher) Publish(topic string, messages []message.ProducedMessage) error {
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

	mp.produced = append(mp.produced, messages...)
	return nil
}

func (mp *mockPublisher) ClosePublisher() error {
	mp.closed = true
	return nil
}

func (mp *mockPublisher) PopMessages() []message.ProducedMessage {
	defer func() { mp.produced = []message.ProducedMessage{} }()
	return mp.produced
}

var handlerFuncAlwaysOKMessages = []message.ProducedMessage{
	NewMockProducedMessage(42),
	NewMockProducedMessage(3.141592653589793238),
}

func handlerFuncAlwaysOK(message.ConsumedMessage) ([]message.ProducedMessage, error) {
	return handlerFuncAlwaysOKMessages, nil
}

func handlerFuncAlwaysFailing(message.ConsumedMessage) ([]message.ProducedMessage, error) {
	return nil, errFailed
}

func handlerFuncAlwaysPanicking(message.ConsumedMessage) ([]message.ProducedMessage, error) {
	panic(errPanicked)
}

type mockConsumedMessage struct {
	uuid     string
	metadata map[string]string
	ack      chan error
}

// NewMockConsumedMessage returns a ConsumedMessage that always succeeds with everything.
// Caveat: UnmarshalPayload doesn't really unmarshal anything, not sure how it should actually do it.
func NewMockConsumedMessage(uuid string, metadata map[string]string) message.ConsumedMessage {
	return &mockConsumedMessage{uuid, metadata, make(chan error)}
}

func (mcm mockConsumedMessage) UUID() string {
	return mcm.uuid
}

func (mcm mockConsumedMessage) GetMetadata(key string) string {
	return mcm.metadata[key]
}

func (mcm mockConsumedMessage) AllMetadata() map[string]string {
	return mcm.metadata
}

func (mcm mockConsumedMessage) UnmarshalPayload(val interface{}) error {
	// no idea what to do really; hope nobody relies on the payload being unmarshaled successfully
	return nil
}

func (mcm mockConsumedMessage) Acknowledged() <-chan error {
	return mcm.ack
}

func (mcm *mockConsumedMessage) Acknowledge() error {
	close(mcm.ack)
	return nil
}

func (mockConsumedMessage) Error(err error) error {
	return nil
}
