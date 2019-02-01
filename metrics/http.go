package metrics

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServeHTTP establishes an HTTP server that exposes the /metrics endpoint for Prometheus at the given address.
// It returns the prometheus registry (to register the metrics on) and a canceling function that ends the server.
func ServeHTTP(addr string) (registry *prometheus.Registry, cancel func()) {
	registry = prometheus.NewRegistry()
	return registry, ServeHTTPWithRegistry(addr, registry)
}

// ServeHTTP establishes an HTTP server that exposes the /metrics endpoint for Prometheus at the given address.
// It takes an existing Prometheus registry and returns a canceling function that ends the server.
func ServeHTTPWithRegistry(addr string, registry *prometheus.Registry) (cancel func()) {
	router := chi.NewRouter()

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	router.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
	})
	server := http.Server{
		Addr:    addr,
		Handler: handler,
	}

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()

	wait := make(chan struct{})
	go func() {
		<-wait
		server.Close()
	}()

	return func() { close(wait) }
}
