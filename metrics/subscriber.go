package metrics

import (
	"errors"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	subscriberLabelKeys = []string{
		labelKeyHandlerName,
		labelKeySubscriberName,
	}
)

type SubscriberPrometheusMetricsDecorator struct {
	sub message.Subscriber

	subscriberReceivedTotal    *prometheus.CounterVec
	subscriberTimeToAckSeconds *prometheus.HistogramVec
	subscriberCountTotal       prometheus.Gauge

	outputChannels     []chan *message.Message
	outputChannelsLock sync.Mutex

	closed  bool
	closing chan struct{}
}

func (s *SubscriberPrometheusMetricsDecorator) Subscribe(topic string) (chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.outputChannelsLock.Lock()
	defer s.outputChannelsLock.Unlock()

	out := make(chan *message.Message)

	in, err := s.sub.Subscribe(topic)
	if err != nil {
		return nil, err
	}

	s.outputChannels = append(s.outputChannels, out)

	go func() {
		for {
			select {
			case msg := <-in:
				if msg == nil {
					continue
				}
				s.recordMetrics(msg)
				out <- msg

			case <-s.closing:
				return
			}
		}
	}()

	return out, nil
}

func (s SubscriberPrometheusMetricsDecorator) recordMetrics(msg *message.Message) {
	now := time.Now()
	// todo: sub receives this message before the handler, so handler had no chance to write ctx. Investigate...
	ctx := msg.Context()
	labels := labelsFromCtx(ctx, subscriberLabelKeys...)

	s.subscriberReceivedTotal.With(labels).Inc()

	go func() {
		select {
		case <-msg.Acked():
			s.subscriberTimeToAckSeconds.With(labels).Observe(time.Since(now).Seconds())
		case <-s.closing:
			return
		}
	}()
}

func (s SubscriberPrometheusMetricsDecorator) Close() error {
	if s.closed {
		return nil
	}

	close(s.closing)

	s.outputChannelsLock.Lock()
	for _, ch := range s.outputChannels {
		close(ch)
	}
	s.outputChannels = nil
	s.outputChannelsLock.Unlock()

	s.subscriberCountTotal.Dec()

	return s.sub.Close()
}

// DecorateSubscriber wraps the underlying subscriber with Prometheus metrics.
func (b PrometheusMetricsBuilder) DecorateSubscriber(sub message.Subscriber) (wrapped message.Subscriber, err error) {
	subscriberReceivedTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_received_total",
			Help:      "Total number of received messages",
		},
		subscriberLabelKeys,
	)

	subscriberTimeToAckSeconds := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_time_to_ack_seconds",
			Help:      "The time elapsed between obtaining a message and receiving an ACK",
		},
		subscriberLabelKeys,
	)

	subscriberCountTotal := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_count_total",
			Help:      "The total count of active subscribers",
		},
	)

	for _, c := range []prometheus.Collector{
		subscriberReceivedTotal,
		subscriberTimeToAckSeconds,
		// subscriberCountTotal is WIP, don't register yet
	} {
		if registerErr := b.PrometheusRegistry.Register(c); registerErr != nil {
			err = multierror.Append(err, registerErr)
		}
	}
	if err != nil {
		return nil, err
	}

	subscriberCountTotal.Inc()
	return &SubscriberPrometheusMetricsDecorator{
		sub:                        sub,
		subscriberReceivedTotal:    subscriberReceivedTotal,
		subscriberTimeToAckSeconds: subscriberTimeToAckSeconds,
		subscriberCountTotal:       subscriberCountTotal,

		outputChannels: []chan (*message.Message){},
	}, nil
}
