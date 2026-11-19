package topology

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type describedPubSub struct {
	pubSub PubSub
}

func (d *describedPubSub) DescribeTopic(topic string) Description {
	return Description{
		PubSub:                d.pubSub,
		Resources:             []Resource{{Kind: "topic", Name: "physical-" + topic}},
		SubscriptionResources: []Resource{{Kind: "consumer group", Name: "group"}},
	}
}

// plainPubSub has a field, because pointers to zero-size values may be equal.
type plainPubSub struct{ name string }

func TestRegistry_nil(t *testing.T) {
	var r *Registry

	assert.Empty(t, r.AddNode(Node{Kind: NodeRouter}))
	assert.Empty(t, r.TopicID(&plainPubSub{}, "topic"))
	assert.Empty(t, r.AddSubscription(&plainPubSub{}, "topic", "handler"))
	assert.Empty(t, r.ObjectID(&plainPubSub{}, "prefix"))
	r.AddEdge("a", "b", EdgePublish)
	r.Annotate(&plainPubSub{}, "name", map[string]string{"a": "b"})
	r.Remove(&plainPubSub{})
	r.RemoveNode("a")
	r.ReplacePubSub("a", PubSub{ID: "b"})
	r.Report(Event{Kind: EventPublished})
	r.ReportPublished(&plainPubSub{}, "topic", context.Background(), "uuid", nil, nil)
	r.CapturePayloads(true)
	assert.Empty(t, r.Snapshot().Nodes)
	assert.Empty(t, r.Stats())

	events, unsubscribe := r.SubscribeEvents(1)
	_, open := <-events
	assert.False(t, open)
	unsubscribe()
}

func TestRegistry_TopicID(t *testing.T) {
	r := newRegistry()

	described := &describedPubSub{pubSub: PubSub{ID: "kafka-1", Type: "kafka", Name: "Kafka"}}

	topicID := r.TopicID(described, "orders")
	assert.Equal(t, "kafka-1/orders", topicID)
	assert.Equal(t, topicID, r.TopicID(described, "orders"), "the node should be added once")

	plainTopicID := r.TopicID(&plainPubSub{}, "orders")
	assert.Equal(t, "pubsub-topology/orders", plainTopicID, "a Pub/Sub that is not a Describer is identified by its package")

	snapshot := r.Snapshot()
	require.Len(t, snapshot.Nodes, 4)

	pubSub := findNode(t, snapshot, "kafka-1")
	assert.Equal(t, NodePubSub, pubSub.Kind)
	assert.False(t, pubSub.Local)

	topic := findNode(t, snapshot, topicID)
	assert.Equal(t, NodeTopic, topic.Kind)
	assert.Equal(t, "orders", topic.Name)
	assert.Equal(t, "kafka-1", topic.Parent)
	assert.Equal(t, []Resource{{Kind: "topic", Name: "physical-orders"}}, topic.Resources)
	assert.False(t, topic.Local)
}

func TestRegistry_AddSubscription(t *testing.T) {
	r := newRegistry()

	described := &describedPubSub{pubSub: PubSub{ID: "kafka-1"}}
	handlerID := r.AddNode(Node{Kind: NodeHandler, Name: "handler"})

	topicID := r.AddSubscription(described, "orders", handlerID)
	assert.Equal(t, "kafka-1/orders", topicID)

	edges := r.Snapshot().Edges
	require.Len(t, edges, 1)
	assert.Equal(t, Edge{
		From:      topicID,
		To:        handlerID,
		Kind:      EdgeSubscribe,
		Resources: []Resource{{Kind: "consumer group", Name: "group"}},
	}, edges[0])

	r.IgnoreTopic("ignored")
	assert.Empty(t, r.AddSubscription(described, "ignored", handlerID))
	assert.Len(t, r.Snapshot().Edges, 1)
}

func TestRegistry_ReportPublished(t *testing.T) {
	r := newRegistry()
	events, unsubscribe := r.SubscribeEvents(10)
	defer unsubscribe()

	pubSub := &describedPubSub{pubSub: PubSub{ID: "gochannel-1"}}
	handlerID := r.AddNode(Node{Kind: NodeHandler, Name: "handler"})

	ctx := ContextWithOrigin(context.Background(), Origin{NodeID: handlerID, CausationUUID: "parent-uuid"})
	ctx, tracker := TrackPublish(ctx)
	assert.False(t, tracker.Reported())

	r.ReportPublished(pubSub, "orders", ctx, "uuid", map[string]string{"key": "value"}, []byte("payload"))
	assert.True(t, tracker.Reported())

	event := receiveEvent(t, events)
	assert.Equal(t, EventPublished, event.Kind)
	assert.Equal(t, handlerID, event.NodeID)
	assert.Equal(t, "gochannel-1/orders", event.TopicID)
	assert.Equal(t, "parent-uuid", event.CausationUUID)
	assert.Equal(t, "uuid", event.Message.UUID)
	assert.Equal(t, map[string]string{"key": "value"}, event.Message.Metadata)
	assert.Empty(t, event.Message.Payload, "payloads are not captured by default")
	assert.False(t, event.Message.PayloadCaptured)
	assert.Equal(t, len("payload"), event.Message.PayloadSize)
	assert.False(t, event.Time.IsZero())

	edges := r.Snapshot().Edges
	require.Len(t, edges, 1)
	assert.Equal(t, Edge{From: handlerID, To: "gochannel-1/orders", Kind: EdgePublish, Dynamic: true}, edges[0])

	assert.Equal(t, []Stats{{NodeID: "gochannel-1/orders", Published: 1}}, r.Stats())

	r.IgnoreTopic("ignored")
	r.ReportPublished(pubSub, "ignored", context.Background(), "uuid", nil, nil)
	assertNoEvent(t, events)
}

func TestRegistry_CapturePayloads(t *testing.T) {
	r := newRegistry()
	r.CapturePayloads(true)

	metadata := map[string]string{"key": "value"}
	payload := []byte("payload")

	message := r.CopyMessage("uuid", metadata, payload)
	assert.Equal(t, payload, message.Payload)
	assert.True(t, message.PayloadCaptured)

	payload[0] = 'P'
	metadata["key"] = "changed"
	assert.Equal(t, []byte("payload"), message.Payload, "the payload should be copied")
	assert.Equal(t, "value", message.Metadata["key"], "the metadata should be copied")

	large := []byte(strings.Repeat("a", MaxPayloadSize+10))
	message = r.CopyMessage("uuid", nil, large)
	assert.Len(t, message.Payload, MaxPayloadSize)
	assert.Equal(t, MaxPayloadSize+10, message.PayloadSize)
}

func TestRegistry_Stats(t *testing.T) {
	r := newRegistry()

	r.Report(Event{Kind: EventHandled, NodeID: "handler", Duration: time.Second})
	r.Report(Event{Kind: EventFailed, NodeID: "handler", Duration: 2 * time.Second})
	r.Report(Event{Kind: EventReceived, NodeID: "handler"})

	assert.Equal(t, []Stats{{NodeID: "handler", Handled: 2, Failed: 1, HandlingTime: 3 * time.Second}}, r.Stats())
}

func TestRegistry_SubscribeEvents(t *testing.T) {
	r := newRegistry()

	events, unsubscribe := r.SubscribeEvents(2)

	r.Report(Event{Kind: EventReceived, NodeID: "1"})
	r.Report(Event{Kind: EventReceived, NodeID: "2"})
	r.Report(Event{Kind: EventReceived, NodeID: "3"})

	first := receiveEvent(t, events)
	second := receiveEvent(t, events)
	assert.Equal(t, "1", first.NodeID)
	assert.Equal(t, "2", second.NodeID)
	assert.Less(t, first.Seq, second.Seq)
	assertNoEvent(t, events)

	unsubscribe()
	r.Report(Event{Kind: EventReceived, NodeID: "4"})
	assertNoEvent(t, events)
}

func TestRegistry_Annotate(t *testing.T) {
	r := newRegistry()

	router := &plainPubSub{name: "router"}
	id := r.AddNode(Node{Kind: NodeRouter, Name: "Router", Labels: map[string]string{"a": "1"}}, router)
	assert.Equal(t, id, r.NodeID(router))

	before := r.Snapshot()
	r.Annotate(router, "FanIn", map[string]string{"b": "2"})

	node := findNode(t, r.Snapshot(), id)
	assert.Equal(t, "FanIn", node.Name)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, node.Labels)
	assert.Equal(t, map[string]string{"a": "1"}, findNode(t, before, id).Labels, "earlier snapshots should not change")
	assert.Greater(t, r.Version(), before.Version)
}

func TestRegistry_Remove(t *testing.T) {
	r := newRegistry()

	pubSub := &describedPubSub{pubSub: PubSub{ID: "gochannel-1"}}
	router := &plainPubSub{name: "router"}
	otherRouter := &plainPubSub{name: "other router"}

	routerID := r.AddNode(Node{Kind: NodeRouter}, router)
	handlerID := r.AddNode(Node{ID: routerID + "/handler", Kind: NodeHandler, Parent: routerID})
	r.AddSubscription(pubSub, "only-this-router", handlerID)
	r.AddEdge(handlerID, r.TopicID(pubSub, "shared"), EdgePublish)

	otherRouterID := r.AddNode(Node{Kind: NodeRouter}, otherRouter)
	otherHandlerID := r.AddNode(Node{ID: otherRouterID + "/handler", Kind: NodeHandler, Parent: otherRouterID})
	r.AddSubscription(pubSub, "shared", otherHandlerID)

	r.Report(Event{Kind: EventHandled, NodeID: handlerID})

	r.Remove(router)

	assert.Equal(t, []string{"gochannel-1", "gochannel-1/shared", otherRouterID, otherHandlerID}, nodeIDs(r.Snapshot()))
	assert.Empty(t, r.NodeID(router))
	assert.Empty(t, r.Stats(), "stats of removed nodes should be removed")

	r.Remove(otherRouter)
	assert.Empty(t, r.Snapshot().Nodes, "topics with no edges and Pub/Subs with no topics should be removed")
	assert.Empty(t, r.Snapshot().Edges)
}

func TestRegistry_ReplacePubSub(t *testing.T) {
	r := newRegistry()

	temporary := PubSub{ID: "kafka-localhost:9092", Type: "kafka", Name: "Kafka localhost:9092"}
	pubSub := &describedPubSub{pubSub: temporary}

	handlerID := r.AddNode(Node{Kind: NodeHandler})
	oldTopicID := r.AddSubscription(pubSub, "orders", handlerID)
	r.Report(Event{Kind: EventPublished, TopicID: oldTopicID})

	identified := PubSub{ID: "kafka-cluster", Type: "kafka", Name: "Kafka cluster"}
	r.ReplacePubSub(temporary.ID, identified)

	assert.Equal(t, []string{handlerID, "kafka-cluster", "kafka-cluster/orders"}, sortedNodeIDs(r.Snapshot()))
	assert.Equal(t, "Kafka cluster", findNode(t, r.Snapshot(), "kafka-cluster").Name)
	assert.Equal(t, "kafka-cluster", findNode(t, r.Snapshot(), "kafka-cluster/orders").Parent)

	edges := r.Snapshot().Edges
	require.Len(t, edges, 1)
	assert.Equal(t, "kafka-cluster/orders", edges[0].From)

	assert.Equal(t, []Stats{{NodeID: "kafka-cluster/orders", Published: 1}}, r.Stats(), "stats should be moved")

	events, unsubscribe := r.SubscribeEvents(1)
	defer unsubscribe()
	r.Report(Event{Kind: EventReceived, NodeID: handlerID, TopicID: oldTopicID})
	assert.Equal(t, "kafka-cluster/orders", receiveEvent(t, events).TopicID, "events with old topic IDs should be translated")

	assert.Equal(t, "kafka-cluster/orders", r.TopicID(pubSub, "orders"), "a Describer that returns the old ID should get the new one")
	assert.Equal(t, "kafka-cluster/payments", r.TopicID(pubSub, "payments"))

	// Replacing with a Pub/Sub that was replaced itself ends at the last one.
	r.ReplacePubSub("local", temporary)
	assert.Equal(t, "kafka-cluster/orders", r.TopicID(&describedPubSub{pubSub: PubSub{ID: "local"}}, "orders"))
}

func TestReportFailure(t *testing.T) {
	assert.Empty(t, ReportedFailure(context.Background()))

	// Without ContextWithFailureReport, there's nowhere to report to.
	ReportFailure(context.Background(), errors.New("failed"))

	ctx := ContextWithFailureReport(context.Background())
	assert.Empty(t, ReportedFailure(ctx))

	ReportFailure(ctx, nil)
	assert.Empty(t, ReportedFailure(ctx))

	ReportFailure(ctx, errors.New("failed"))
	assert.Equal(t, "failed", ReportedFailure(ctx))
}

func TestEnable(t *testing.T) {
	Disable()
	t.Cleanup(Disable)

	assert.Nil(t, Current())

	r := Enable()
	assert.Same(t, r, Current())
	assert.Same(t, r, Enable(), "Enable should be idempotent")

	Disable()
	assert.Nil(t, Current())
	assert.NotSame(t, r, Enable(), "Enable after Disable should create a new registry")
}

func findNode(t *testing.T, snapshot Snapshot, id string) Node {
	t.Helper()
	for _, node := range snapshot.Nodes {
		if node.ID == id {
			return node
		}
	}
	t.Fatalf("node %s not found in %v", id, nodeIDs(snapshot))
	return Node{}
}

func nodeIDs(snapshot Snapshot) []string {
	ids := make([]string, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func sortedNodeIDs(snapshot Snapshot) []string {
	ids := nodeIDs(snapshot)
	slices.Sort(ids)
	return ids
}

func receiveEvent(t *testing.T, events <-chan Event) Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("no event")
		return Event{}
	}
}

func assertNoEvent(t *testing.T, events <-chan Event) {
	t.Helper()
	select {
	case event, ok := <-events:
		if ok {
			t.Fatalf("unexpected event: %+v", event)
		}
	default:
	}
}
