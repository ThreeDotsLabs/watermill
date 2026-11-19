// Package inspector serves a live view of the Watermill setup of the running process:
// routers, handlers, Pub/Subs and topics as an interactive graph, with messages flowing through it.
//
// Create the inspector before creating routers and Pub/Subs, as only components created later are visible:
//
//	inspectorHandler, err := inspector.New(inspector.Config{})
//	// ...
//	mux.Handle("/watermill/", http.StripPrefix("/watermill", inspectorHandler))
//
// With Config.Discovery, inspectors of many services find each other, and each of them shows all services.
//
// The inspector shows metadata of messages (and payloads, if enabled) and has no authentication for browsers.
// It is a development tool.
//
// EXPERIMENTAL: this package is a proof of concept and its API will change.
package inspector

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/topology"
)

//go:embed index.html
var indexHTML []byte

// protocolVersion is the version of the stream that inspectors of different services send to each other.
const protocolVersion = 1

// maxStreams is the number of event streams that can be open at the same time.
const maxStreams = 32

// Config is the configuration of the inspector.
type Config struct {
	// ServiceName is the name of this service in the inspector. Instances of one service should have the same name.
	// Defaults to the name of the binary.
	ServiceName string

	// EventBuffer is the number of the most recent events kept in memory for clients that connect later.
	// Older events are dropped. Defaults to 500.
	EventBuffer int

	// Payloads enables showing payloads of messages.
	//
	// By default, payloads are not copied, kept or sent to the browser: only their size is shown.
	// When enabled, up to topology.MaxPayloadSize bytes of each payload are kept with the buffered events.
	Payloads bool

	// Discovery lets the inspectors of many services find each other. Then the inspector of any service shows
	// all of them. It's optional. When set, Token and AdvertiseURL are required.
	Discovery Discovery

	// Token is a secret shared by all services. It protects the data that inspectors send to each other.
	Token string

	// AdvertiseURL is the URL of this handler that other services can reach, for example "http://orders:8787/".
	AdvertiseURL string

	// Logger is optional.
	Logger watermill.LoggerAdapter
}

func (c *Config) setDefaults() {
	if c.ServiceName == "" {
		c.ServiceName = filepath.Base(os.Args[0])
	}
	if c.EventBuffer <= 0 {
		c.EventBuffer = 500
	}
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func (c Config) validate() error {
	if c.Discovery == nil {
		return nil
	}

	if c.Token == "" {
		return errors.New("token is required with discovery")
	}
	if c.AdvertiseURL == "" {
		return errors.New("advertise URL is required with discovery")
	}
	if _, err := peerStreamURL(c.AdvertiseURL); err != nil {
		return fmt.Errorf("invalid advertise URL: %w", err)
	}

	return nil
}

// Inspector serves the inspector UI and its data. It is an http.Handler.
type Inspector struct {
	config   Config
	registry *topology.Registry
	instance string
	mux      *http.ServeMux
	peers    *peers
	cancel   context.CancelFunc

	streams atomic.Int32

	// eventsLock guards the recent events and the subscribers of new events, so that a subscriber
	// gets the recent events and then every new one, with none missing in between.
	eventsLock       sync.Mutex
	recent           []topology.Event
	eventSubscribers map[uint64]chan topology.Event
	nextSubscriber   uint64
}

// New enables the topology registry and returns the inspector.
//
// The handler can be mounted under any path.
func New(config Config) (*Inspector, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	i := &Inspector{
		config:   config,
		registry: topology.Enable(),
		instance: newID(),
		mux:      http.NewServeMux(),
		cancel:   cancel,

		eventSubscribers: map[uint64]chan topology.Event{},
	}
	i.registry.CapturePayloads(config.Payloads)

	events, _ := i.registry.SubscribeEvents(4096)
	go i.collect(events)

	if config.Discovery != nil {
		var err error
		i.peers, err = startPeers(ctx, config.Discovery, config.Token, Announcement{
			Service:  config.ServiceName,
			Instance: i.instance,
			URL:      config.AdvertiseURL,
		}, config.Logger)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("cannot start discovery: %w", err)
		}
	}

	i.mux.HandleFunc("GET /{$}", i.index)
	i.mux.HandleFunc("GET /api/events", i.events)
	i.mux.HandleFunc("GET /api/local/events", i.localEvents)

	return i, nil
}

func (i *Inspector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	i.mux.ServeHTTP(w, r)
}

// Close stops announcing this service to others.
func (i *Inspector) Close() error {
	i.cancel()
	return nil
}

func newID() string {
	random := make([]byte, 6)
	_, _ = rand.Read(random)
	return hex.EncodeToString(random)
}

// collect keeps the most recent events, so they can be replayed to new clients,
// and passes the events to the streams. A stream that is too slow loses events.
func (i *Inspector) collect(events <-chan topology.Event) {
	for event := range events {
		i.eventsLock.Lock()
		if len(i.recent) >= i.config.EventBuffer {
			copy(i.recent, i.recent[1:])
			i.recent = i.recent[:len(i.recent)-1]
		}
		i.recent = append(i.recent, event)

		for _, subscriber := range i.eventSubscribers {
			select {
			case subscriber <- event:
			default:
			}
		}
		i.eventsLock.Unlock()
	}
}

// subscribeEvents returns the recent events and a channel with the events that come after them.
// The returned function ends the subscription.
func (i *Inspector) subscribeEvents(buffer int) ([]topology.Event, <-chan topology.Event, func()) {
	i.eventsLock.Lock()
	defer i.eventsLock.Unlock()

	ch := make(chan topology.Event, buffer)
	id := i.nextSubscriber
	i.nextSubscriber++
	i.eventSubscribers[id] = ch

	unsubscribe := func() {
		i.eventsLock.Lock()
		delete(i.eventSubscribers, id)
		i.eventsLock.Unlock()
	}

	return append([]topology.Event(nil), i.recent...), ch, unsubscribe
}

func (i *Inspector) index(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

// source identifies an instance of a service.
type source struct {
	Service  string `json:"service"`
	Instance string `json:"instance"`
}

// item is a topology, an event or the stats of one instance.
type item struct {
	source   source
	topology *topology.Snapshot
	event    *wireEvent
	stats    map[string]wireStats
}

// wireStats are the totals of a node since the start of its process (or processes, when merged).
// Rates are calculated from them by the browser.
type wireStats struct {
	Published  uint64  `json:"published,omitempty"`
	Handled    uint64  `json:"handled,omitempty"`
	Failed     uint64  `json:"failed,omitempty"`
	HandlingMs float64 `json:"handlingMs,omitempty"`
}

func (s wireStats) add(other wireStats) wireStats {
	s.Published += other.Published
	s.Handled += other.Handled
	s.Failed += other.Failed
	s.HandlingMs += other.HandlingMs
	return s
}

const statsInterval = time.Second

func (i *Inspector) localStats() map[string]wireStats {
	stats := map[string]wireStats{}
	for _, node := range i.registry.Stats() {
		stats[node.NodeID] = wireStats{
			Published:  node.Published,
			Handled:    node.Handled,
			Failed:     node.Failed,
			HandlingMs: float64(node.HandlingTime.Microseconds()) / 1000,
		}
	}
	return stats
}

// streamLocal sends the topology and the events of this process to out, until ctx is done.
// It starts with the current topology and the recent events. The topology is always sent before
// the events that refer to it.
func (i *Inspector) streamLocal(ctx context.Context, out chan<- item) {
	self := source{Service: i.config.ServiceName, Instance: i.instance}

	recent, live, unsubscribe := i.subscribeEvents(4096)
	defer unsubscribe()

	send := func(it item) bool {
		select {
		case out <- it:
			return true
		case <-ctx.Done():
			return false
		}
	}

	var sentVersion uint64
	sendTopologyIfChanged := func() bool {
		if i.registry.Version() == sentVersion {
			return true
		}
		snapshot := i.registry.Snapshot()
		sentVersion = snapshot.Version
		return send(item{source: self, topology: &snapshot})
	}
	sendEvent := func(event topology.Event) bool {
		wire := toWireEvent(event)
		return send(item{source: self, event: &wire})
	}

	sendStats := func() bool {
		return sendTopologyIfChanged() && send(item{source: self, stats: i.localStats()})
	}

	if !sendStats() {
		return
	}

	for _, event := range recent {
		if !sendEvent(event) {
			return
		}
	}

	topologyTicker := time.NewTicker(300 * time.Millisecond)
	defer topologyTicker.Stop()
	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-live:
			if !sendTopologyIfChanged() || !sendEvent(event) {
				return
			}
		case <-topologyTicker.C:
			if !sendTopologyIfChanged() {
				return
			}
		case <-statsTicker.C:
			if !sendStats() {
				return
			}
		}
	}
}

// startStream prepares the response for Server-Sent Events.
// The returned function has to be called when the stream ends.
func (i *Inspector) startStream(w http.ResponseWriter) (http.Flusher, func(), bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return nil, nil, false
	}

	if i.streams.Add(1) > maxStreams {
		i.streams.Add(-1)
		http.Error(w, "too many open streams", http.StatusServiceUnavailable)
		return nil, nil, false
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	return flusher, func() { i.streams.Add(-1) }, true
}

// localEvents streams the topology and the events of this process to the inspectors of other services.
func (i *Inspector) localEvents(w http.ResponseWriter, r *http.Request) {
	token := i.config.Token
	if token == "" {
		// Without a token, the data of this process is not available to other services.
		http.NotFound(w, r)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	flusher, done, ok := i.startStream(w)
	if !ok {
		return
	}
	defer done()

	items := make(chan item, 256)
	go i.streamLocal(r.Context(), items)

	hello := peerHello{
		source:   source{Service: i.config.ServiceName, Instance: i.instance},
		Protocol: protocolVersion,
	}
	if err := writeSSE(w, "hello", hello); err != nil {
		return
	}
	flusher.Flush()

	keepAliveTicker := time.NewTicker(15 * time.Second)
	defer keepAliveTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case it := <-items:
			// Sending everything that is waiting before flushing.
			for {
				var err error
				switch {
				case it.topology != nil:
					err = writeSSE(w, "topology", it.topology)
				case it.stats != nil:
					err = writeSSE(w, "stats", it.stats)
				default:
					err = writeSSE(w, "message", it.event)
				}
				if err != nil {
					return
				}

				select {
				case it = <-items:
					continue
				default:
				}
				break
			}
			flusher.Flush()

		case <-keepAliveTicker.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// events streams the topology and the events of all known services to the browser.
//
// Every connection is a separate session: it connects to the other services on its own
// and disconnects when the browser goes away.
func (i *Inspector) events(w http.ResponseWriter, r *http.Request) {
	flusher, done, ok := i.startStream(w)
	if !ok {
		return
	}
	defer done()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	items := make(chan item, 1024)
	go i.streamLocal(ctx, items)

	// The browser drops what it has when it sees a new session.
	if err := writeSSE(w, "hello", map[string]string{"session": newID()}); err != nil {
		return
	}
	flusher.Flush()

	merged := newMerger()
	var seq uint64

	// Streams of other instances, by instance.
	type peerStream struct {
		url    string
		cancel context.CancelFunc
	}
	peerStreams := map[string]peerStream{}
	syncPeers := func() (changed bool) {
		alive := i.peers.list()

		for instance, p := range alive {
			if stream, ok := peerStreams[instance]; ok {
				if stream.url == p.URL {
					continue
				}
				// The instance has announced a new address. Its topology stays until the new stream sends one.
				stream.cancel()
			}
			peerCtx, cancelPeer := context.WithCancel(ctx)
			peerStreams[instance] = peerStream{url: p.URL, cancel: cancelPeer}
			go i.streamPeer(peerCtx, p, items)
		}

		for instance, stream := range peerStreams {
			if _, ok := alive[instance]; ok {
				continue
			}
			stream.cancel()
			delete(peerStreams, instance)
			if merged.removeInstance(instance) {
				changed = true
			}
		}

		return changed
	}
	syncPeers()

	write := func(it item) error {
		// An item of an instance that is already gone.
		if _, ok := peerStreams[it.source.Instance]; !ok && it.source.Instance != i.instance {
			return nil
		}

		if it.topology != nil {
			merged.setTopology(it.source, *it.topology)
			return writeSSE(w, "topology", merged.topology())
		}
		if it.stats != nil {
			// Stats of all instances are sent together, by the ticker.
			merged.setStats(it.source, it.stats)
			return nil
		}

		seq++
		return writeSSE(w, "message", merged.event(it.source, *it.event, seq))
	}

	peersTicker := time.NewTicker(2 * time.Second)
	defer peersTicker.Stop()
	keepAliveTicker := time.NewTicker(15 * time.Second)
	defer keepAliveTicker.Stop()
	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-statsTicker.C:
			if err := writeSSE(w, "stats", merged.mergedStats()); err != nil {
				return
			}
			flusher.Flush()

		case it := <-items:
			// Sending everything that is waiting before flushing.
			for {
				if err := write(it); err != nil {
					return
				}

				select {
				case it = <-items:
					continue
				default:
				}
				break
			}
			flusher.Flush()

		case <-peersTicker.C:
			if syncPeers() {
				if err := writeSSE(w, "topology", merged.topology()); err != nil {
					return
				}
				flusher.Flush()
			}

		case <-keepAliveTicker.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, encoded)
	return err
}

// peerStreamURL returns the URL of the stream of an inspector with the given address.
func peerStreamURL(inspectorURL string) (string, error) {
	parsed, err := url.Parse(inspectorURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("unsupported scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", errors.New("missing host")
	}

	return parsed.JoinPath("api/local/events").String(), nil
}

// peerHello is the first thing in the stream between inspectors.
type peerHello struct {
	source
	Protocol int `json:"protocol"`
}

// wireEvent is an event in the form sent to browsers and to other inspectors.
type wireEvent struct {
	Seq           uint64  `json:"seq"`
	Kind          string  `json:"kind"`
	Time          int64   `json:"time"`
	Node          string  `json:"node,omitempty"`
	Topic         string  `json:"topic,omitempty"`
	UUID          string  `json:"uuid"`
	CausationUUID string  `json:"causation,omitempty"`
	Error         string  `json:"error,omitempty"`
	DurationMs    float64 `json:"durationMs,omitempty"`

	// Service and Instance say where the event happened. They are set when events of many services are merged.
	Service  string `json:"service,omitempty"`
	Instance string `json:"instance,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`
	// Payload is the text of the payload. It's empty for a payload that is not a valid UTF-8 text.
	Payload          string `json:"payload,omitempty"`
	PayloadSize      int    `json:"payloadSize,omitempty"`
	PayloadTruncated bool   `json:"payloadTruncated,omitempty"`
	PayloadBinary    bool   `json:"payloadBinary,omitempty"`
	// PayloadHidden is true when the message has a payload, but payloads are not enabled in the config.
	PayloadHidden bool `json:"payloadHidden,omitempty"`
}

func toWireEvent(event topology.Event) wireEvent {
	out := wireEvent{
		Seq:           event.Seq,
		Kind:          string(event.Kind),
		Time:          event.Time.UnixMilli(),
		Node:          event.NodeID,
		Topic:         event.TopicID,
		UUID:          event.Message.UUID,
		CausationUUID: event.CausationUUID,
		Error:         event.Error,
		DurationMs:    float64(event.Duration.Microseconds()) / 1000,

		Metadata:    event.Message.Metadata,
		PayloadSize: event.Message.PayloadSize,
	}

	switch {
	case !event.Message.PayloadCaptured:
		out.PayloadHidden = event.Message.PayloadSize > 0
	case utf8.Valid(event.Message.Payload):
		out.Payload = string(event.Message.Payload)
		out.PayloadTruncated = event.Message.PayloadSize > len(event.Message.Payload)
	default:
		out.PayloadBinary = true
	}

	return out
}
