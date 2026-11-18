package cmd

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestProducePublishesLinesOfAnyLength(t *testing.T) {
	longLine := strings.Repeat("x", 100_000)
	stdin := pipeWithInput(t, "first\n"+longLine+"\nlast\n")

	publisher, done := startProduce(t, stdin)

	if err := waitForProduce(t, done); err != nil {
		t.Fatalf("produce returned error: %v", err)
	}
	assertPublished(t, publisher.drain(), []string{"first", longLine, "last"})
}

func TestProduceExitsAtEndOfInput(t *testing.T) {
	stdin := pipeWithInput(t, "first\nlast line without newline")

	publisher, done := startProduce(t, stdin)

	if err := waitForProduce(t, done); err != nil {
		t.Fatalf("produce returned error: %v", err)
	}
	assertPublished(t, publisher.drain(), []string{"first", "last line without newline"})
}

func TestProducePublishesLinesBeforeEndOfInput(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })

	publisher, done := startProduce(t, r)

	// Input that stays open, like `tail -f file | mill ... produce`.
	if _, err := w.WriteString("first\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-publisher.payloads:
		if payload != "first" {
			t.Fatalf("published %q, want %q", payload, "first")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("line was not published within 5s while the input was still open")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := waitForProduce(t, done); err != nil {
		t.Fatalf("produce returned error: %v", err)
	}
}

func TestProduceFailsWhenStdinCannotBeRead(t *testing.T) {
	// Reading a directory fails with EISDIR, like `mill ... produce < some-directory`.
	dir, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dir.Close() })

	_, done := startProduce(t, dir)

	err = waitForProduce(t, done)
	if err == nil {
		t.Fatal("produce returned no error, want the read error")
	}
	if !strings.Contains(err.Error(), "stdin") {
		t.Errorf("error %q does not mention stdin", err)
	}
}

type recordingPublisher struct {
	payloads chan string
}

func (p recordingPublisher) Publish(_ string, messages ...*message.Message) error {
	for _, msg := range messages {
		p.payloads <- string(msg.Payload)
	}
	return nil
}

func (p recordingPublisher) Close() error {
	return nil
}

func (p recordingPublisher) drain() []string {
	var payloads []string
	for {
		select {
		case payload := <-p.payloads:
			payloads = append(payloads, payload)
		default:
			return payloads
		}
	}
}

func startProduce(t *testing.T, stdin *os.File) (recordingPublisher, <-chan error) {
	t.Helper()

	logger = watermill.NopLogger{}
	publisher := recordingPublisher{payloads: make(chan string, 100)}
	producer = publisher
	viper.Set("test.topic", "test-topic")

	oldStdin := os.Stdin
	os.Stdin = stdin
	t.Cleanup(func() { os.Stdin = oldStdin })

	produceCmd := addProduceCmd(&cobra.Command{Use: "test"}, "test.topic")

	done := make(chan error, 1)
	go func() {
		done <- produceCmd.RunE(produceCmd, nil)
	}()

	return publisher, done
}

func waitForProduce(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("produce did not finish within 5s after its input ended")
		return nil
	}
}

func pipeWithInput(t *testing.T, input string) *os.File {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })

	go func() {
		_, _ = w.WriteString(input)
		_ = w.Close()
	}()

	return r
}

func assertPublished(t *testing.T, got, want []string) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("published %d messages %v, want %d messages %v", len(got), summarize(got), len(want), summarize(want))
	}
}

// summarize shortens long payloads, so failure messages stay readable.
func summarize(payloads []string) []string {
	summary := make([]string, len(payloads))
	for i, payload := range payloads {
		if len(payload) > 20 {
			payload = fmt.Sprintf("%.10s...(%d bytes)", payload, len(payload))
		}
		summary[i] = payload
	}
	return summary
}
