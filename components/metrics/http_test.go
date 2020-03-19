package metrics_test

import (
	"net/http"
	"testing"

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
