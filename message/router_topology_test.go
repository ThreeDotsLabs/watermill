package message_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/ThreeDotsLabs/watermill/topology"
)

// enableTopology enables a fresh topology registry for the test.
// Tests that use it can't run in parallel with other tests, because the registry is global.
func enableTopology(tb testing.TB) *topology.Registry {
	topology.Disable()
	tb.Cleanup(topology.Disable)
	return topology.Enable()
}

func TestRouter_topology(t *testing.T) {
	registry := enableTopology(t)
	events, unsubscribe := registry.SubscribeEvents(100)
	defer unsubscribe()

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})
	defer func() { _ = pubSub.Close() }()

	router, err := message.NewRouter(message.RouterConfig{}, watermill.NopLogger{})
	require.NoError(t, err)

	router.AddHandler("forward", "in", pubSub, "out", pubSub, func(msg *message.Message) ([]*message.Message, error) {
		return []*message.Message{message.NewMessage("produced", msg.Payload)}, nil
	})
	router.AddConsumerHandler("consume", "out", pubSub, func(msg *message.Message) error {
		return nil
	})

	routerID := registry.NodeID(router)
	require.NotEmpty(t, routerID)
	forwardID := routerID + "/forward"
	consumeID := routerID + "/consume"
	pubSubID := registry.ObjectID(pubSub, "gochannel")
	inID := pubSubID + "/in"
	outID := pubSubID + "/out"

	snapshot := registry.Snapshot()
	assert.ElementsMatch(t, []string{routerID, forwardID, consumeID, pubSubID, inID, outID}, nodeIDs(snapshot))
	assert.ElementsMatch(t, []topology.Edge{
		{From: inID, To: forwardID, Kind: topology.EdgeSubscribe},
		{From: forwardID, To: outID, Kind: topology.EdgePublish},
		{From: outID, To: consumeID, Kind: topology.EdgeSubscribe},
	}, snapshot.Edges)

	runRouter(t, router)

	require.NoError(t, pubSub.Publish("in", message.NewMessage("original", []byte("payload"))))

	received := waitForEvent(t, events, func(e topology.Event) bool {
		return e.Kind == topology.EventHandled && e.NodeID == consumeID
	})
	// The forward handler may ack after the consume handler has received the produced message.
	received = append(received, waitForEvent(t, events, func(e topology.Event) bool {
		return e.Kind == topology.EventHandled && e.NodeID == forwardID
	}, received...)...)

	published := filterEvents(received, topology.EventPublished)
	require.Len(t, published, 2, "every publish should be reported once")
	assert.Equal(t, "original", published[0].Message.UUID)
	assert.Equal(t, inID, published[0].TopicID)
	assert.Empty(t, published[0].NodeID, "the message was published outside of handlers")
	assert.Equal(t, "produced", published[1].Message.UUID)
	assert.Equal(t, outID, published[1].TopicID)
	assert.Equal(t, forwardID, published[1].NodeID)
	assert.Equal(t, "original", published[1].CausationUUID)

	assertEvent(t, received, topology.EventReceived, forwardID, inID, "original")
	assertEvent(t, received, topology.EventHandled, forwardID, inID, "original")
	assertEvent(t, received, topology.EventReceived, consumeID, outID, "produced")
	assertEvent(t, received, topology.EventHandled, consumeID, outID, "produced")

	require.NoError(t, router.Close())
	assert.Empty(t, registry.Snapshot().Nodes, "the router, its handlers and topics that are not used anymore should be removed")
}

func TestRouter_topology_failures(t *testing.T) {
	registry := enableTopology(t)
	events, unsubscribe := registry.SubscribeEvents(100)
	defer unsubscribe()

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})
	defer func() { _ = pubSub.Close() }()

	router, err := message.NewRouter(message.RouterConfig{}, watermill.NopLogger{})
	require.NoError(t, err)

	// The handlers fail on the first attempt. The nacked message is delivered again.
	var failedOnceLock sync.Mutex
	failedOnce := map[string]bool{}
	failOnce := func(name string) bool {
		failedOnceLock.Lock()
		defer failedOnceLock.Unlock()

		if failedOnce[name] {
			return false
		}
		failedOnce[name] = true
		return true
	}

	router.AddConsumerHandler("error", "error", pubSub, func(msg *message.Message) error {
		if failOnce("error") {
			return errors.New("handler failed")
		}
		return nil
	})
	router.AddConsumerHandler("panic", "panic", pubSub, func(msg *message.Message) error {
		if failOnce("panic") {
			panic("handler panicked")
		}
		return nil
	})

	poisonQueue, err := middleware.PoisonQueue(pubSub, "poison")
	require.NoError(t, err)
	router.AddConsumerHandler("poison", "poison_source", pubSub, func(msg *message.Message) error {
		return errors.New("handler failed")
	}).AddMiddleware(poisonQueue)

	routerID := registry.NodeID(router)
	pubSubID := registry.ObjectID(pubSub, "gochannel")

	runRouter(t, router)

	t.Run("error", func(t *testing.T) {
		require.NoError(t, pubSub.Publish("error", message.NewMessage("error-msg", nil)))

		received := waitForEvent(t, events, func(e topology.Event) bool {
			return e.Kind == topology.EventHandled && e.NodeID == routerID+"/error"
		})
		failed := assertEvent(t, received, topology.EventFailed, routerID+"/error", pubSubID+"/error", "error-msg")
		assert.Equal(t, "handler failed", failed.Error)
	})

	t.Run("panic", func(t *testing.T) {
		require.NoError(t, pubSub.Publish("panic", message.NewMessage("panic-msg", nil)))

		received := waitForEvent(t, events, func(e topology.Event) bool {
			return e.Kind == topology.EventHandled && e.NodeID == routerID+"/panic"
		})
		failed := assertEvent(t, received, topology.EventFailed, routerID+"/panic", pubSubID+"/panic", "panic-msg")
		assert.Equal(t, "panic: handler panicked", failed.Error)
	})

	t.Run("poison_queue", func(t *testing.T) {
		require.NoError(t, pubSub.Publish("poison_source", message.NewMessage("poison-msg", nil)))

		received := waitForEvent(t, events, func(e topology.Event) bool {
			return e.Kind == topology.EventFailed && e.NodeID == routerID+"/poison"
		})
		failed := received[len(received)-1]
		assert.Equal(t, "handler failed", failed.Error, "a failure dealt with by the middleware should be reported")

		published := filterEvents(received, topology.EventPublished)
		require.NotEmpty(t, published)
		poisoned := published[len(published)-1]
		assert.Equal(t, pubSubID+"/poison", poisoned.TopicID)
		assert.Equal(t, routerID+"/poison", poisoned.NodeID)

		for _, node := range registry.Snapshot().Nodes {
			if node.ID == pubSubID+"/poison" {
				assert.Equal(t, topology.RolePoisonQueue, node.Labels[topology.LabelRole])
				return
			}
		}
		t.Error("the poison queue topic should be in the topology")
	})

	stats := map[string]topology.Stats{}
	for _, s := range registry.Stats() {
		stats[s.NodeID] = s
	}
	assert.Equal(t, uint64(2), stats[routerID+"/error"].Handled)
	assert.Equal(t, uint64(1), stats[routerID+"/error"].Failed)
	assert.Equal(t, uint64(1), stats[routerID+"/poison"].Failed)
}

// BenchmarkRouterHandler_topology compares handling messages with the topology registry disabled and enabled.
func BenchmarkRouterHandler_topology(b *testing.B) {
	b.Run("disabled", func(b *testing.B) {
		topology.Disable()
		BenchmarkRouterHandler(b)
	})
	b.Run("enabled", func(b *testing.B) {
		enableTopology(b)
		BenchmarkRouterHandler(b)
	})
	b.Run("enabled_with_subscriber", func(b *testing.B) {
		registry := enableTopology(b)
		registry.CapturePayloads(true)

		// Like the inspector, which keeps receiving all events.
		events, unsubscribe := registry.SubscribeEvents(4096)
		defer unsubscribe()
		go func() {
			for range events {
			}
		}()

		BenchmarkRouterHandler(b)
	})
}

func runRouter(t *testing.T, router *message.Router) {
	t.Helper()

	go func() {
		_ = router.Run(context.Background())
	}()
	t.Cleanup(func() { _ = router.Close() })

	select {
	case <-router.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("router not running")
	}
}

// waitForEvent returns the events received until the one that matches, including it.
// It also checks the events received earlier.
func waitForEvent(t *testing.T, events <-chan topology.Event, match func(topology.Event) bool, earlier ...topology.Event) []topology.Event {
	t.Helper()

	for _, event := range earlier {
		if match(event) {
			return nil
		}
	}

	var received []topology.Event
	timeout := time.After(5 * time.Second)
	for {
		select {
		case event := <-events:
			received = append(received, event)
			if match(event) {
				return received
			}
		case <-timeout:
			t.Fatalf("event not received, got: %+v", received)
			return nil
		}
	}
}

func filterEvents(events []topology.Event, kind topology.EventKind) []topology.Event {
	var filtered []topology.Event
	for _, event := range events {
		if event.Kind == kind {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func assertEvent(t *testing.T, events []topology.Event, kind topology.EventKind, nodeID, topicID, uuid string) topology.Event {
	t.Helper()

	for _, event := range events {
		if event.Kind == kind && event.NodeID == nodeID && event.TopicID == topicID && event.Message.UUID == uuid {
			return event
		}
	}
	t.Errorf("no %s event of %s for %s on %s in %+v", kind, uuid, nodeID, topicID, events)
	return topology.Event{}
}

func nodeIDs(snapshot topology.Snapshot) []string {
	ids := make([]string, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		ids = append(ids, node.ID)
	}
	return ids
}
