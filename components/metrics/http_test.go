package metrics_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/components/metrics"
)

func TestCreateRegistryAndServeHTTP_metrics_endpoint(t *testing.T) {
	reg, cancel := metrics.CreateRegistryAndServeHTTP(":8090")
	defer cancel()
	err := reg.Register(collectors.NewBuildInfoCollector())
	if err != nil {
		t.Fatal(fmt.Errorf("registration of prometheus build info collector failed: %w", err))
	}
	waitServerReady(t, "http://localhost:8090")
	resp, err := http.DefaultClient.Get("http://localhost:8090/metrics")
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		t.Fatal(fmt.Errorf("call to metrics endpoint failed: %w", err))
	}
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCreateRegistryAndServeHTTP_unknown_endpoint(t *testing.T) {
	reg, cancel := metrics.CreateRegistryAndServeHTTP(":8091")
	defer cancel()
	err := reg.Register(collectors.NewBuildInfoCollector())
	if err != nil {
		t.Error(fmt.Errorf("registration of prometheus build info collector failed: %w", err))
	}
	waitServerReady(t, "http://localhost:8091")
	resp, err := http.DefaultClient.Get("http://localhost:8091/unknown")
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		t.Fatal(fmt.Errorf("call to unknown endpoint failed: %w", err))
	}
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// server might have small delay before being able to server traffic
func waitServerReady(t *testing.T, addr string) {
	for i := 0; i < 50; i++ {
		_, err := http.DefaultClient.Get(addr)
		// assume server ready when no err anymore
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
		continue
	}
}
