package manager

import (
	"context"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
)

type mockDriver struct {
	topics map[string]chan *message.Message
	mu     sync.Mutex
}

func newMockDriver() *mockDriver {
	return &mockDriver{
		topics: make(map[string]chan *message.Message),
	}
}

func (m *mockDriver) Publish(topic string, messages ...*message.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[topic]; !ok {
		m.topics[topic] = make(chan *message.Message, 1)
	}

	for _, msg := range messages {
		m.topics[topic] <- msg
	}

	return nil
}

func (m *mockDriver) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.topics[topic]; !ok {
		m.topics[topic] = make(chan *message.Message, 1)
	}

	return m.topics[topic], nil
}

func (m *mockDriver) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ch := range m.topics {
		close(ch)
	}

	return nil
}

var _ Driver = (*mockDriver)(nil)

func TestManager(t *testing.T) {
	w := New(newMockDriver())
	w.Register("mock", newMockDriver())
	w.Register("mock2", newMockDriver())

	if w.Use("mock") == nil {
		t.Fatal("expected driver")
	}

	if w.Use("mock2") == nil {
		t.Fatal("expected driver")
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected no panic")
			}
		}()

		w.Use("mock3")
	}()

	for _, d := range []Driver{
		w, w.Use(), w.Use("mock"), w.Use("mock2"),
	} {
		var wg sync.WaitGroup

		wg.Add(1)
		go func(d Driver) {
			wg.Done()
			msg, err := w.Subscribe(context.Background(), "mock")
			if err != nil {
				t.Fatal(err)
			}

			m := <-msg
			if string(m.Payload) != "1" {
				t.Error("expected 1")
				return
			}
		}(d)

		if err := d.Publish("mock", message.NewMessage("1", []byte("1"))); err != nil {
			t.Fatal(err)
		}

		wg.Wait()
	}

}
