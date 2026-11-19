package cmd

import (
	"io"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/pubsub" //nolint:staticcheck // printSubscriptionInfo takes the v1 type used by googlecloud.go
)

func TestPrintSubscriptionInfoPrintsPercentSignsVerbatim(t *testing.T) {
	endpoint := "https://example.com/push?next=%2Fdone&ratio=100%"

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldStdout := os.Stdout
	os.Stdout = w
	err = printSubscriptionInfo("projects/p/subscriptions/s", pubsub.SubscriptionConfig{
		PushConfig: pubsub.PushConfig{Endpoint: endpoint},
	})
	os.Stdout = oldStdout
	if closeErr := w.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatalf("printSubscriptionInfo returned error: %v", err)
	}

	output, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), endpoint) {
		t.Errorf("output does not contain the endpoint %q verbatim:\n%s", endpoint, output)
	}
}
