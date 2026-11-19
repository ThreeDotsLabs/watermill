// Package topology is an optional, process-wide registry of Watermill components and the messages
// flowing between them. It is the data source of the inspector UI (see components/inspector).
//
// It is disabled by default. While disabled, Current returns nil and the instrumentation in Watermill
// is reduced to a nil check. All Registry methods are safe to call on a nil Registry.
//
// EXPERIMENTAL: this package is a proof of concept and its API will change.
package topology

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MaxPayloadSize is the maximum number of payload bytes kept for a single observed message.
const MaxPayloadSize = 16 * 1024

var current atomic.Pointer[Registry]

// Enable creates the registry and returns it. It is idempotent.
//
// Only components created after Enable are visible, so it should be called first.
func Enable() *Registry {
	for {
		if r := current.Load(); r != nil {
			return r
		}
		r := newRegistry()
		if current.CompareAndSwap(nil, r) {
			return r
		}
	}
}

// Disable drops the registry. Components created while it was enabled stop reporting to it.
// It's mostly useful in tests, which can start with a fresh registry by calling Disable and then Enable.
func Disable() {
	current.Store(nil)
}

// Current returns the registry, or nil when it's not enabled.
func Current() *Registry {
	return current.Load()
}

type NodeKind string

const (
	NodeRouter  NodeKind = "router"
	NodeHandler NodeKind = "handler"
	NodeBus     NodeKind = "bus"
	NodePubSub  NodeKind = "pubsub"
	NodeTopic   NodeKind = "topic"
)

type EdgeKind string

const (
	EdgeSubscribe EdgeKind = "subscribe"
	EdgePublish   EdgeKind = "publish"
)

// Label keys and values that the inspector UI understands.
const (
	// LabelComponent is the Watermill component that a router or a handler belongs to, for example "FanIn".
	LabelComponent = "component"
	// LabelRole is the role of a topic, for example RolePoisonQueue.
	LabelRole = "role"

	RolePoisonQueue = "poison queue"
)

// Node is a single element of the topology.
type Node struct {
	ID   string   `json:"id"`
	Kind NodeKind `json:"kind"`
	Name string   `json:"name"`
	// Type is the Go type or the kind of system behind the node, for example "gochannel".
	Type string `json:"type,omitempty"`
	// Parent is the ID of the node this node belongs to: a router for a handler, a Pub/Sub for a topic.
	Parent string            `json:"parent,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
	// Resources are the physical resources behind a topic, as reported by a Describer.
	Resources []Resource `json:"resources,omitempty"`
	// Local is true for Pub/Subs that exist only in this process, like GoChannel, and for their topics.
	// When topologies of many services are merged, other Pub/Subs and topics with the same ID become one node.
	Local bool `json:"local,omitempty"`
}

// Edge connects two nodes. Subscribe edges go from a topic, publish edges go to a topic.
type Edge struct {
	From string   `json:"from"`
	To   string   `json:"to"`
	Kind EdgeKind `json:"kind"`
	// Dynamic is true when the edge was not declared, but discovered from a published message.
	Dynamic bool `json:"dynamic,omitempty"`
	// Resources are the physical resources of a subscription, as reported by a Describer.
	Resources []Resource `json:"resources,omitempty"`
}

// Snapshot is the state of the topology at one moment: all known nodes and edges.
type Snapshot struct {
	Version uint64 `json:"version"`
	Nodes   []Node `json:"nodes"`
	Edges   []Edge `json:"edges"`
}

// Describer can be implemented by a Publisher or a Subscriber to describe what is behind a Watermill topic.
//
// A Pub/Sub that implements Describer should also report the messages it publishes with Registry.ReportPublished.
// DescribeTopic is called for every published message, so it should be cheap.
type Describer interface {
	DescribeTopic(topic string) Description
}

// Description describes a Watermill topic of a specific Pub/Sub.
type Description struct {
	PubSub PubSub
	// Resources are the physical resources behind the topic that are shared by all publishers and subscribers,
	// for example a Kafka topic or an exchange.
	Resources []Resource
	// SubscriptionResources are the physical resources that belong to the described subscriber only,
	// for example a consumer group or a queue. Publishers leave it empty.
	SubscriptionResources []Resource
}

// PubSub identifies the system that messages go through.
// A publisher and a subscriber that are connected to the same system should return the same ID.
type PubSub struct {
	ID   string
	Type string
	Name string
	// Local is true for a Pub/Sub that exists only in this process, like GoChannel.
	// Other Pub/Subs, like a Kafka cluster, may be shared by many services: when their topologies are merged,
	// topics of Pub/Subs with the same ID become one node.
	Local bool
}

// Resource is a physical resource of a Pub/Sub.
type Resource struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type EventKind string

const (
	// EventPublished is reported when a message is published to a topic.
	EventPublished EventKind = "published"
	// EventReceived is reported when a handler receives a message.
	EventReceived EventKind = "received"
	// EventHandled is reported when a handler has handled a message with no error.
	EventHandled EventKind = "handled"
	// EventFailed is reported when a handler has failed to handle a message. It's about the handler, not the broker:
	// a message that a middleware has dealt with, like the poison queue, is acked, but its handling has failed
	// (see ReportFailure).
	EventFailed EventKind = "failed"
)

// Event is something that happened to a message.
type Event struct {
	Seq  uint64
	Kind EventKind
	Time time.Time
	// NodeID is the publishing node for EventPublished (it may be empty) and the handler for other events.
	NodeID  string
	TopicID string
	Message Message
	// CausationUUID is the UUID of the message that was being handled when this one was published.
	CausationUUID string
	Error         string
	Duration      time.Duration
}

// Message is a copy of the observed message.
type Message struct {
	UUID     string
	Metadata map[string]string
	// Payload is cut to MaxPayloadSize. It's empty unless payloads are captured (see Registry.CapturePayloads).
	Payload         []byte
	PayloadSize     int
	PayloadCaptured bool
}

// CapturePayloads decides if payloads of observed messages are copied. By default, they are not:
// only the size of the payload is known.
func (r *Registry) CapturePayloads(capture bool) {
	if r == nil {
		return
	}
	r.capturePayloads.Store(capture)
}

// CopyMessage copies the message data, so that it's safe to keep.
func (r *Registry) CopyMessage(uuid string, metadata map[string]string, payload []byte) Message {
	m := Message{UUID: uuid, PayloadSize: len(payload)}
	if len(metadata) > 0 {
		m.Metadata = make(map[string]string, len(metadata))
		for k, v := range metadata {
			m.Metadata[k] = v
		}
	}
	if r == nil || !r.capturePayloads.Load() {
		return m
	}

	if len(payload) > MaxPayloadSize {
		payload = payload[:MaxPayloadSize]
	}
	m.Payload = append([]byte(nil), payload...)
	m.PayloadCaptured = true
	return m
}

// Origin says where messages published with a context come from: the node that publishes them,
// and the message it is handling, if any. It travels in the message context, so that a message
// published while handling another one can be linked to it.
type Origin struct {
	NodeID string
	// CausationUUID is the UUID of the message that the node is handling.
	CausationUUID string
}

type originKey struct{}

// ContextWithOrigin returns a context with the origin of messages published with it.
func ContextWithOrigin(ctx context.Context, origin Origin) context.Context {
	return context.WithValue(ctx, originKey{}, origin)
}

// OriginFromContext returns the origin set with ContextWithOrigin, or an empty Origin.
func OriginFromContext(ctx context.Context) Origin {
	origin, _ := ctx.Value(originKey{}).(Origin)
	return origin
}

type failureKey struct{}

// ContextWithFailureReport returns a context of a handled message that ReportFailure can be used with.
func ContextWithFailureReport(ctx context.Context) context.Context {
	return context.WithValue(ctx, failureKey{}, &atomic.Pointer[string]{})
}

// ReportFailure tells that handling of the message has failed, although the handler will return no error.
// It's for middlewares that deal with the error on their own, like the poison queue.
// ctx is the context of the handled message.
func ReportFailure(ctx context.Context, err error) {
	if failure, ok := ctx.Value(failureKey{}).(*atomic.Pointer[string]); ok && err != nil {
		reason := err.Error()
		failure.Store(&reason)
	}
}

// ReportedFailure returns the error passed to ReportFailure, or an empty string.
func ReportedFailure(ctx context.Context) string {
	if failure, ok := ctx.Value(failureKey{}).(*atomic.Pointer[string]); ok {
		if reason := failure.Load(); reason != nil {
			return *reason
		}
	}
	return ""
}

// PublishTracker tells if a publish of a message was reported by a layer below.
//
// A publisher is often wrapped: a handler or a bus calls a decorator, which calls the Pub/Sub.
// Only one of them should report the publish, and the one closest to the Pub/Sub knows the most.
// So the outer layer tracks the publish, calls the publisher, and reports the publish only when
// nothing below did it.
type PublishTracker struct {
	reported atomic.Bool
}

// Reported is true when the publish was reported with Registry.ReportPublished using the tracked context.
func (t *PublishTracker) Reported() bool {
	return t.reported.Load()
}

type trackerKey struct{}

// TrackPublish returns a context that should be set to the message before publishing it.
func TrackPublish(ctx context.Context) (context.Context, *PublishTracker) {
	tracker := &PublishTracker{}
	return context.WithValue(ctx, trackerKey{}, tracker), tracker
}

type edgeKey struct {
	from, to string
	kind     EdgeKind
}

// Registry keeps the topology and passes events to subscribers.
type Registry struct {
	seq     atomic.Uint64
	version atomic.Uint64

	capturePayloads atomic.Bool
	ignoredTopics   sync.Map
	// aliases maps IDs of nodes moved by ReplacePubSub to their new IDs.
	aliases sync.Map
	// stats maps node IDs to *nodeStats.
	stats sync.Map

	mu        sync.RWMutex
	nodes     map[string]*Node
	nodeOrder []string
	edges     map[edgeKey]*Edge
	edgeOrder []edgeKey
	// objects maps components to their node IDs. It keeps the components alive until they are removed
	// with Remove, which components call when they are closed.
	objects  map[any]string
	counters map[string]int
	// replacedPubSubs maps old IDs of Pub/Subs to what replaced them (see ReplacePubSub).
	replacedPubSubs map[string]PubSub

	// reportMu is held while an event gets its sequence number and is passed to subscribers,
	// so that every subscriber receives the events in the order of their numbers.
	reportMu sync.Mutex
	subs     map[uint64]chan Event
	nextSub  uint64
}

func newRegistry() *Registry {
	return &Registry{
		nodes:    map[string]*Node{},
		edges:    map[edgeKey]*Edge{},
		objects:  map[any]string{},
		counters: map[string]int{},

		replacedPubSubs: map[string]PubSub{},
		subs:            map[uint64]chan Event{},
	}
}

// objectKey returns a value identifying the component that can be used as a map key.
func objectKey(obj any) (any, bool) {
	if obj == nil {
		return nil, false
	}
	if reflect.ValueOf(obj).Kind() != reflect.Pointer {
		return nil, false
	}
	return obj, true
}

// typeName returns the type of the component without the pointer marker, for example "kafka.Publisher".
func typeName(obj any) string {
	return strings.TrimLeft(fmt.Sprintf("%T", obj), "*")
}

func (r *Registry) nextID(prefix string) string {
	r.counters[prefix]++
	return fmt.Sprintf("%s-%d", prefix, r.counters[prefix])
}

// ObjectID returns a stable ID of the component, like "gochannel-1". The ID is assigned on the first call.
// The component must be a pointer.
func (r *Registry) ObjectID(obj any, prefix string) string {
	if r == nil {
		return ""
	}
	key, ok := objectKey(obj)
	if !ok {
		return ""
	}

	// It's called for every published message, so the common case takes only a read lock.
	r.mu.RLock()
	id, ok := r.objects[key]
	r.mu.RUnlock()
	if ok {
		return id
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if id, ok := r.objects[key]; ok {
		return id
	}
	id = r.nextID(prefix)
	r.objects[key] = id
	return id
}

// AddNode adds a node and returns its ID. When the ID is empty, it's generated from the kind.
// The components passed as bind can be later used with NodeID and Annotate.
func (r *Registry) AddNode(node Node, bind ...any) string {
	if r == nil {
		return ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if node.ID == "" {
		node.ID = r.nextID(string(node.Kind))
	}
	r.addNodeLocked(node)
	for _, obj := range bind {
		if key, ok := objectKey(obj); ok {
			r.objects[key] = node.ID
		}
	}
	return node.ID
}

func (r *Registry) addNodeLocked(node Node) {
	if _, ok := r.nodes[node.ID]; ok {
		return
	}
	r.nodes[node.ID] = &node
	r.nodeOrder = append(r.nodeOrder, node.ID)
	r.version.Add(1)
}

// NodeID returns the ID of the node bound to the component, or an empty string.
func (r *Registry) NodeID(obj any) string {
	if r == nil {
		return ""
	}
	key, ok := objectKey(obj)
	if !ok {
		return ""
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.objects[key]
}

// Annotate changes the name (if not empty) and adds labels to the node bound to the component.
func (r *Registry) Annotate(obj any, name string, labels map[string]string) {
	r.AnnotateNode(r.NodeID(obj), name, labels)
}

// AnnotateNode changes the name (if not empty) and adds labels to the node.
func (r *Registry) AnnotateNode(id string, name string, labels map[string]string) {
	if r == nil || id == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	node, ok := r.nodes[id]
	if !ok {
		return
	}
	if name != "" {
		node.Name = name
	}
	if len(labels) > 0 {
		// The map is replaced, not changed, because snapshots share it.
		merged := make(map[string]string, len(node.Labels)+len(labels))
		for k, v := range node.Labels {
			merged[k] = v
		}
		for k, v := range labels {
			merged[k] = v
		}
		node.Labels = merged
	}
	r.version.Add(1)
}

// Remove removes the node bound to the component (with AddNode or ObjectID), the nodes that belong to it,
// like the handlers of a router or the topics of a Pub/Sub, and their edges. Components call it when they are closed.
//
// Topics that are left with no edges, and Pub/Subs that are left with no topics, are removed too,
// so a closed router doesn't leave its topics behind.
func (r *Registry) Remove(obj any) {
	if r == nil {
		return
	}
	key, ok := objectKey(obj)
	if !ok {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	id, ok := r.objects[key]
	if !ok {
		return
	}
	delete(r.objects, key)
	r.removeNodeLocked(id)
}

// RemoveNode is like Remove, but takes the ID of the node.
func (r *Registry) RemoveNode(id string) {
	if r == nil || id == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.removeNodeLocked(id)
}

func (r *Registry) removeNodeLocked(id string) {
	if _, ok := r.nodes[id]; !ok {
		return
	}

	// The node and everything that belongs to it, directly or not.
	removed := map[string]bool{id: true}
	for found := true; found; {
		found = false
		for nodeID, node := range r.nodes {
			if !removed[nodeID] && removed[node.Parent] {
				removed[nodeID] = true
				found = true
			}
		}
	}

	// Components bound to these nodes are forgotten. Topics and Pub/Subs removed below because they are left
	// empty stay bound, so that a Pub/Sub that is still open keeps its ID when it's used again.
	unbind := maps.Clone(removed)

	connected := map[string]bool{}
	for key := range r.edges {
		if removed[key.from] || removed[key.to] {
			connected[key.from] = true
			connected[key.to] = true
			delete(r.edges, key)
		}
	}

	// Topics that were connected only to the removed nodes, and then their Pub/Subs that have no topics left.
	parents := map[string]bool{}
	for nodeID := range connected {
		node := r.nodes[nodeID]
		if removed[nodeID] || node == nil || node.Kind != NodeTopic || r.hasEdgesLocked(nodeID) {
			continue
		}
		removed[nodeID] = true
		parents[node.Parent] = true
	}
	for parentID := range parents {
		if parent := r.nodes[parentID]; parent != nil && parent.Kind == NodePubSub && !r.hasChildrenLocked(parentID, removed) {
			removed[parentID] = true
		}
	}

	for nodeID := range removed {
		delete(r.nodes, nodeID)
		r.stats.Delete(nodeID)
	}
	r.nodeOrder = slices.DeleteFunc(r.nodeOrder, func(nodeID string) bool { return removed[nodeID] })
	r.edgeOrder = slices.DeleteFunc(r.edgeOrder, func(key edgeKey) bool {
		_, ok := r.edges[key]
		return !ok
	})
	for obj, nodeID := range r.objects {
		if unbind[nodeID] {
			delete(r.objects, obj)
		}
	}

	r.version.Add(1)
}

func (r *Registry) hasEdgesLocked(id string) bool {
	for key := range r.edges {
		if key.from == id || key.to == id {
			return true
		}
	}
	return false
}

// hasChildrenLocked tells if any node that is not going to be removed belongs to the node.
func (r *Registry) hasChildrenLocked(id string, removed map[string]bool) bool {
	for nodeID, node := range r.nodes {
		if node.Parent == id && !removed[nodeID] {
			return true
		}
	}
	return false
}

// AddEdge adds an edge between two nodes. It is idempotent.
func (r *Registry) AddEdge(from, to string, kind EdgeKind) {
	r.addEdge(from, to, kind, false, nil)
}

// AddSubscription adds an edge from the topic of the given subscriber to the node and returns the ID of the topic node.
// If the subscriber implements Describer, the edge has the resources of the subscription.
func (r *Registry) AddSubscription(subscriber any, topic string, nodeID string) string {
	if r == nil || r.isIgnored(topic) {
		return ""
	}

	topicID := r.TopicID(subscriber, topic)

	var resources []Resource
	if describer, ok := subscriber.(Describer); ok {
		resources = describer.DescribeTopic(topic).SubscriptionResources
	}
	r.addEdge(topicID, nodeID, EdgeSubscribe, false, resources)

	return topicID
}

func (r *Registry) addEdge(from, to string, kind EdgeKind, dynamic bool, resources []Resource) {
	if r == nil || from == "" || to == "" {
		return
	}
	key := edgeKey{from, to, kind}

	r.mu.RLock()
	_, ok := r.edges[key]
	r.mu.RUnlock()
	if ok {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.edges[key]; ok {
		return
	}
	r.edges[key] = &Edge{From: from, To: to, Kind: kind, Dynamic: dynamic, Resources: resources}
	r.edgeOrder = append(r.edgeOrder, key)
	r.version.Add(1)
}

// TopicID returns the ID of the node of the topic in the given Pub/Sub. The node is added on the first call.
//
// pubSub is a Publisher or a Subscriber. If it implements Describer, the description is used.
// Otherwise, all publishers and subscribers from one Go package are assumed to be the same Pub/Sub.
//
// The registry doesn't keep pubSub, so it's safe to call it with short-lived publishers
// (unless the Describer of pubSub binds it with ObjectID, like GoChannel does until it's closed).
//
// It's called for every published message, so DescribeTopic should be cheap.
func (r *Registry) TopicID(pubSub any, topic string) string {
	if r == nil || r.isIgnored(topic) {
		return ""
	}

	var description Description
	if describer, ok := pubSub.(Describer); ok {
		description = describer.DescribeTopic(topic)
	}
	if description.PubSub.ID == "" {
		pkg, _, _ := strings.Cut(typeName(pubSub), ".")
		description.PubSub = PubSub{ID: "pubsub-" + pkg, Type: pkg, Name: pkg}
	}

	id := description.PubSub.ID + "/" + topic

	r.mu.RLock()
	_, ok := r.nodes[id]
	r.mu.RUnlock()
	if ok {
		return id
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// The Describer may still return the ID that its Pub/Sub had before it was replaced.
	for i := 0; i < maxAliasChain; i++ {
		replacement, ok := r.replacedPubSubs[description.PubSub.ID]
		if !ok {
			break
		}
		description.PubSub = replacement
		id = replacement.ID + "/" + topic
	}

	r.addNodeLocked(Node{
		ID:    description.PubSub.ID,
		Kind:  NodePubSub,
		Name:  description.PubSub.Name,
		Type:  description.PubSub.Type,
		Local: description.PubSub.Local,
	})
	r.addNodeLocked(Node{
		ID:        id,
		Kind:      NodeTopic,
		Name:      topic,
		Type:      description.PubSub.Type,
		Parent:    description.PubSub.ID,
		Resources: description.Resources,
		Local:     description.PubSub.Local,
	})

	return id
}

// maxAliasChain limits following the replacements of one Pub/Sub, in case they form a cycle.
const maxAliasChain = 8

func (r *Registry) resolveAlias(id string) string {
	for i := 0; i < maxAliasChain && id != ""; i++ {
		newID, ok := r.aliases.Load(id)
		if !ok {
			break
		}
		id = newID.(string)
	}
	return id
}

// ReplacePubSub moves the Pub/Sub with the given ID, with its topics, edges and stats, to the ID of pubSub.
// When nodes with the new IDs already exist, the moved nodes are merged into them.
//
// It's for a Describer that learns the identity of its Pub/Sub only after it was first described,
// for example when the identity is read from the broker in the background. Until then, the Describer
// returns a temporary ID. Later events and descriptions with the old IDs are moved to the new ones.
func (r *Registry) ReplacePubSub(oldID string, pubSub PubSub) {
	if r == nil || oldID == "" || pubSub.ID == "" || oldID == pubSub.ID {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// The new Pub/Sub may have been replaced too.
	for i := 0; i < maxAliasChain; i++ {
		replacement, ok := r.replacedPubSubs[pubSub.ID]
		if !ok {
			break
		}
		pubSub = replacement
	}
	if oldID == pubSub.ID {
		return
	}

	r.replacedPubSubs[oldID] = pubSub

	// Old ID to new ID of every moved node.
	moved := map[string]string{oldID: pubSub.ID}
	for id, node := range r.nodes {
		if node.Kind == NodeTopic && node.Parent == oldID {
			moved[id] = pubSub.ID + "/" + node.Name
		}
	}

	for oldNodeID, newNodeID := range moved {
		r.aliases.Store(oldNodeID, newNodeID)

		node, ok := r.nodes[oldNodeID]
		if !ok {
			continue
		}
		delete(r.nodes, oldNodeID)
		if _, exists := r.nodes[newNodeID]; exists {
			continue
		}

		node.ID = newNodeID
		node.Type = pubSub.Type
		node.Local = pubSub.Local
		if node.Kind == NodePubSub {
			node.Name = pubSub.Name
		} else {
			node.Parent = pubSub.ID
		}
		r.nodes[newNodeID] = node
	}

	movedID := func(id string) string {
		if newID, ok := moved[id]; ok {
			return newID
		}
		return id
	}

	seenNodes := map[string]bool{}
	nodeOrder := make([]string, 0, len(r.nodeOrder))
	for _, id := range r.nodeOrder {
		id = movedID(id)
		if !seenNodes[id] {
			seenNodes[id] = true
			nodeOrder = append(nodeOrder, id)
		}
	}
	r.nodeOrder = nodeOrder

	seenEdges := map[edgeKey]bool{}
	edgeOrder := make([]edgeKey, 0, len(r.edgeOrder))
	for _, key := range r.edgeOrder {
		newKey := edgeKey{from: movedID(key.from), to: movedID(key.to), kind: key.kind}
		if newKey != key {
			edge := r.edges[key]
			delete(r.edges, key)

			if existing, ok := r.edges[newKey]; ok {
				// A declared edge stays declared.
				existing.Dynamic = existing.Dynamic && edge.Dynamic
				if len(existing.Resources) == 0 {
					existing.Resources = edge.Resources
				}
			} else {
				edge.From, edge.To = newKey.from, newKey.to
				r.edges[newKey] = edge
			}
		}
		if !seenEdges[newKey] {
			seenEdges[newKey] = true
			edgeOrder = append(edgeOrder, newKey)
		}
	}
	r.edgeOrder = edgeOrder

	for obj, id := range r.objects {
		r.objects[obj] = movedID(id)
	}

	for oldNodeID, newNodeID := range moved {
		value, ok := r.stats.LoadAndDelete(oldNodeID)
		if !ok {
			continue
		}
		old := value.(*nodeStats)
		stats := r.nodeStats(newNodeID)
		stats.published.Add(old.published.Load())
		stats.handled.Add(old.handled.Load())
		stats.failed.Add(old.failed.Load())
		stats.handlingTime.Add(old.handlingTime.Load())
	}

	r.version.Add(1)
}

// IgnoreTopic makes the registry skip the topic with the given name: it has no node and its messages
// are not reported. It's used for topics that are not a part of the application, like the discovery
// topic of the inspector.
func (r *Registry) IgnoreTopic(topic string) {
	if r == nil {
		return
	}
	r.ignoredTopics.Store(topic, struct{}{})
}

func (r *Registry) isIgnored(topic string) bool {
	_, ok := r.ignoredTopics.Load(topic)
	return ok
}

// Snapshot returns the current topology.
func (r *Registry) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshot := Snapshot{
		Version: r.version.Load(),
		Nodes:   make([]Node, 0, len(r.nodeOrder)),
		Edges:   make([]Edge, 0, len(r.edgeOrder)),
	}
	for _, id := range r.nodeOrder {
		snapshot.Nodes = append(snapshot.Nodes, *r.nodes[id])
	}
	for _, key := range r.edgeOrder {
		snapshot.Edges = append(snapshot.Edges, *r.edges[key])
	}
	return snapshot
}

// Version changes every time the topology changes.
func (r *Registry) Version() uint64 {
	if r == nil {
		return 0
	}
	return r.version.Load()
}

// Report passes the event to all subscribers. It never blocks: a subscriber that is too slow loses events.
func (r *Registry) Report(event Event) {
	if r == nil {
		return
	}

	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	// Handlers keep the topic IDs they got when they were added, which may have been replaced since.
	event.TopicID = r.resolveAlias(event.TopicID)
	if event.Kind == EventPublished && event.NodeID != "" {
		r.addEdge(event.NodeID, event.TopicID, EdgePublish, true, nil)
	}
	r.count(event)

	// Subscribers skip events with numbers they have already seen, so an event must not overtake
	// one with a lower number. Sending is non-blocking, so the lock is held only briefly.
	r.reportMu.Lock()
	defer r.reportMu.Unlock()

	event.Seq = r.seq.Add(1)
	for _, sub := range r.subs {
		select {
		case sub <- event:
		default:
		}
	}
}

// Stats are the totals of one node since the start of the process.
//
// They are counted for every event, also when subscribers of events are too slow to receive it.
type Stats struct {
	NodeID string
	// Published is the number of messages published to the topic.
	Published uint64
	// Handled is the number of messages that the handler has finished handling, with or without an error.
	Handled uint64
	// Failed is the number of messages that the handler has failed to handle.
	Failed uint64
	// HandlingTime is the total time that the handler has spent on the handled messages.
	HandlingTime time.Duration
}

type nodeStats struct {
	published    atomic.Uint64
	handled      atomic.Uint64
	failed       atomic.Uint64
	handlingTime atomic.Int64
}

func (r *Registry) nodeStats(id string) *nodeStats {
	if stats, ok := r.stats.Load(id); ok {
		return stats.(*nodeStats)
	}
	stats, _ := r.stats.LoadOrStore(id, &nodeStats{})
	return stats.(*nodeStats)
}

func (r *Registry) count(event Event) {
	switch event.Kind {
	case EventPublished:
		if event.TopicID != "" {
			r.nodeStats(event.TopicID).published.Add(1)
		}
	case EventHandled, EventFailed:
		if event.NodeID == "" {
			return
		}
		stats := r.nodeStats(event.NodeID)
		stats.handled.Add(1)
		stats.handlingTime.Add(int64(event.Duration))
		if event.Kind == EventFailed {
			stats.failed.Add(1)
		}
	}
}

// Stats returns the totals of all nodes that had any messages.
func (r *Registry) Stats() []Stats {
	if r == nil {
		return nil
	}

	var all []Stats
	r.stats.Range(func(id, value any) bool {
		stats := value.(*nodeStats)
		all = append(all, Stats{
			NodeID:       id.(string),
			Published:    stats.published.Load(),
			Handled:      stats.handled.Load(),
			Failed:       stats.failed.Load(),
			HandlingTime: time.Duration(stats.handlingTime.Load()),
		})
		return true
	})
	return all
}

// ReportPublished reports a message published to the topic of the given Pub/Sub.
// ctx is the context of the message. It is used to find out what published the message.
func (r *Registry) ReportPublished(pubSub any, topic string, ctx context.Context, uuid string, metadata map[string]string, payload []byte) {
	if r == nil || r.isIgnored(topic) {
		return
	}

	if tracker, ok := ctx.Value(trackerKey{}).(*PublishTracker); ok {
		tracker.reported.Store(true)
	}

	origin := OriginFromContext(ctx)
	r.Report(Event{
		Kind:          EventPublished,
		NodeID:        origin.NodeID,
		TopicID:       r.TopicID(pubSub, topic),
		Message:       r.CopyMessage(uuid, metadata, payload),
		CausationUUID: origin.CausationUUID,
	})
}

// SubscribeEvents returns a channel with all events reported from now on, and a function that ends the subscription.
// Events are dropped when the buffer of the channel is full. The events are received in the order of Event.Seq.
//
// On a nil Registry, the returned channel is closed.
func (r *Registry) SubscribeEvents(buffer int) (<-chan Event, func()) {
	ch := make(chan Event, buffer)
	if r == nil {
		close(ch)
		return ch, func() {}
	}

	r.reportMu.Lock()
	id := r.nextSub
	r.nextSub++
	r.subs[id] = ch
	r.reportMu.Unlock()

	return ch, func() {
		r.reportMu.Lock()
		delete(r.subs, id)
		r.reportMu.Unlock()
	}
}
