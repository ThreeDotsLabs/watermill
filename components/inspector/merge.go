package inspector

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/topology"
)

// maxStreamLine is the longest line read from the stream of another inspector. A topology is one line.
const maxStreamLine = 8 * 1024 * 1024

const peerReconnectInterval = 2 * time.Second

// peerClient doesn't follow redirects: an inspector is asked only at the address it has announced.
var peerClient = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
	Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	},
}

// streamPeer sends the topology and the events of another instance to out, until ctx is done.
// It reconnects when the connection is lost.
func (h *Handler) streamPeer(ctx context.Context, p peer, out chan<- item) {
	// Events are replayed after every connection. lastSeq is used to skip the ones that were already read.
	var lastSeq uint64

	for {
		err := h.readPeer(ctx, p, out, &lastSeq)
		if ctx.Err() != nil {
			return
		}
		h.config.Logger.Debug("Lost the inspector of another service: "+err.Error(), watermill.LogFields{
			"service": p.Service,
			"url":     p.URL,
		})

		select {
		case <-ctx.Done():
			return
		case <-time.After(peerReconnectInterval):
		}
	}
}

func (h *Handler) readPeer(ctx context.Context, p peer, out chan<- item, lastSeq *uint64) error {
	streamURL, err := peerStreamURL(p.URL)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.config.Token)

	resp, err := peerClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	// The service name comes from the signed announcement, not from the stream.
	from := source{Service: p.Service, Instance: p.Instance}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), maxStreamLine)

	var eventName string
	for scanner.Scan() {
		line := scanner.Text()

		if name, ok := strings.CutPrefix(line, "event: "); ok {
			eventName = name
			continue
		}
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}

		var it item
		switch eventName {
		case "hello":
			var hello peerHello
			if err := json.Unmarshal([]byte(data), &hello); err != nil {
				return fmt.Errorf("invalid hello: %w", err)
			}
			if hello.Protocol != protocolVersion {
				return fmt.Errorf("unsupported protocol version %d", hello.Protocol)
			}
			if hello.Instance != p.Instance {
				return fmt.Errorf("another instance (%s) answers at this address", hello.Instance)
			}
			continue

		case "topology":
			var snapshot topology.Snapshot
			if err := json.Unmarshal([]byte(data), &snapshot); err != nil {
				return fmt.Errorf("invalid topology: %w", err)
			}
			it = item{source: from, topology: &snapshot}

		case "stats":
			stats := map[string]wireStats{}
			if err := json.Unmarshal([]byte(data), &stats); err != nil {
				return fmt.Errorf("invalid stats: %w", err)
			}
			it = item{source: from, stats: stats}

		case "message":
			var event wireEvent
			if err := json.Unmarshal([]byte(data), &event); err != nil {
				return fmt.Errorf("invalid event: %w", err)
			}
			if event.Seq <= *lastSeq {
				continue
			}
			*lastSeq = event.Seq
			it = item{source: from, event: &event}

		default:
			continue
		}

		select {
		case out <- it:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return fmt.Errorf("stream ended")
}

// merger joins topologies and events of many instances into one.
//
// Nodes that exist only in one process (routers, handlers, buses, local Pub/Subs) get the name of the service
// in the ID. All instances of one service have the same IDs, so they become one set of nodes.
// Shared Pub/Subs and their topics keep their IDs, so they connect the services.
type merger struct {
	topologies map[source]topology.Snapshot
	// shared tells which nodes of an instance are shared.
	shared map[source]map[string]bool
	stats  map[source]map[string]wireStats
}

func newMerger() *merger {
	return &merger{
		topologies: map[source]topology.Snapshot{},
		shared:     map[source]map[string]bool{},
		stats:      map[source]map[string]wireStats{},
	}
}

func (m *merger) setTopology(from source, snapshot topology.Snapshot) {
	shared := make(map[string]bool, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		shared[node.ID] = node.Shared
	}

	m.topologies[from] = snapshot
	m.shared[from] = shared
}

// removeInstance forgets the instance. It returns true when the merged topology has changed.
func (m *merger) removeInstance(instance string) bool {
	removed := false
	for from := range m.topologies {
		if from.Instance == instance {
			delete(m.topologies, from)
			delete(m.shared, from)
			delete(m.stats, from)
			removed = true
		}
	}
	return removed
}

func (m *merger) setStats(from source, stats map[string]wireStats) {
	m.stats[from] = stats
}

// mergedStats returns the stats with IDs of the merged topology.
// Stats of nodes that become one node, like a handler of two instances or a topic of two services, are added up.
func (m *merger) mergedStats() map[string]wireStats {
	out := map[string]wireStats{}
	for from, stats := range m.stats {
		for id, nodeStats := range stats {
			merged := m.id(from, id)
			out[merged] = out[merged].add(nodeStats)
		}
	}
	return out
}

// id returns the ID that a node of the instance has in the merged topology.
func (m *merger) id(from source, id string) string {
	if id == "" || m.shared[from][id] {
		return id
	}
	return from.Service + "::" + id
}

type mergedNode struct {
	topology.Node
	// Service is the service that the node belongs to. It's empty for shared nodes.
	Service string `json:"service,omitempty"`
}

type mergedService struct {
	Name      string `json:"name"`
	Instances int    `json:"instances"`
}

type mergedTopology struct {
	Services []mergedService `json:"services"`
	Nodes    []mergedNode    `json:"nodes"`
	Edges    []topology.Edge `json:"edges"`
}

func (m *merger) topology() mergedTopology {
	sources := make([]source, 0, len(m.topologies))
	for from := range m.topologies {
		sources = append(sources, from)
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].Service != sources[j].Service {
			return sources[i].Service < sources[j].Service
		}
		return sources[i].Instance < sources[j].Instance
	})

	out := mergedTopology{
		Services: []mergedService{},
		Nodes:    []mergedNode{},
		Edges:    []topology.Edge{},
	}

	type edgeKey struct {
		from, to string
		kind     topology.EdgeKind
	}
	seenNodes := map[string]bool{}
	seenEdges := map[edgeKey]bool{}

	for _, from := range sources {
		if n := len(out.Services); n > 0 && out.Services[n-1].Name == from.Service {
			out.Services[n-1].Instances++
		} else {
			out.Services = append(out.Services, mergedService{Name: from.Service, Instances: 1})
		}

		snapshot := m.topologies[from]

		for _, node := range snapshot.Nodes {
			merged := mergedNode{Node: node}
			merged.ID = m.id(from, node.ID)
			merged.Parent = m.id(from, node.Parent)
			if !node.Shared {
				merged.Service = from.Service
			}

			if seenNodes[merged.ID] {
				continue
			}
			seenNodes[merged.ID] = true
			out.Nodes = append(out.Nodes, merged)
		}

		for _, edge := range snapshot.Edges {
			edge.From = m.id(from, edge.From)
			edge.To = m.id(from, edge.To)

			key := edgeKey{edge.From, edge.To, edge.Kind}
			if seenEdges[key] {
				continue
			}
			seenEdges[key] = true
			out.Edges = append(out.Edges, edge)
		}
	}

	return out
}

// event returns the event of the instance with IDs of the merged topology.
// Events are numbered in the order of arrival, because clocks of services differ.
func (m *merger) event(from source, event wireEvent, seq uint64) wireEvent {
	event.Seq = seq
	event.Node = m.id(from, event.Node)
	event.Topic = m.id(from, event.Topic)
	event.Service = from.Service
	event.Instance = from.Instance
	return event
}
