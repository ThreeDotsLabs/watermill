package metrics_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/metrics"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestCreateRegistryAndServeHTTP_metrics_endpoint(t *testing.T) {
	reg, cancel := metrics.CreateRegistryAndServeHTTP(":8090")
	defer cancel()
	err := reg.Register(prometheus.NewBuildInfoCollector())
	if err != nil {
		t.Fatal(errors.Wrap(err, "registration of prometheus build info collector failed"))
	}
	waitServerReady(t, "http://localhost:8090")
	resp, err := http.DefaultClient.Get("http://localhost:8090/metrics")
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		t.Fatal(errors.Wrap(err, "call to metrics endpoint failed"))
	}
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCreateRegistryAndServeHTTP_unknown_endpoint(t *testing.T) {
	reg, cancel := metrics.CreateRegistryAndServeHTTP(":8091")
	defer cancel()
	err := reg.Register(prometheus.NewBuildInfoCollector())
	if err != nil {
		t.Error(errors.Wrap(err, "registration of prometheus build info collector failed"))
	}
	waitServerReady(t, "http://localhost:8091")
	resp, err := http.DefaultClient.Get("http://localhost:8091/unknown")
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		t.Fatal(errors.Wrap(err, "call to unknown endpoint failed"))
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
