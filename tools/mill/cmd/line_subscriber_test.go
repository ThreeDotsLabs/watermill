package cmd

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLineSubscriberCloseDoesNotWaitForInput(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = w.Close()
		_ = r.Close()
	})

	subscriber := newLineSubscriber(r)
	messages, err := subscriber.Subscribe(context.Background(), "stdin")
	if err != nil {
		t.Fatal(err)
	}

	// Nothing is written, so the read is blocked, as when Ctrl-C is pressed while waiting for input.
	closed := make(chan error, 1)
	go func() {
		closed <- subscriber.Close()
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s while the input was still open")
	}

	if _, ok := <-messages; ok {
		t.Error("received a message after Close, want the output channel closed")
	}
}
