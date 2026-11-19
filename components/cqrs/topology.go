package cqrs

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/topology"
)

// busTopology connects a command or an event bus with the topology registry.
// The zero value means that the topology registry is disabled.
type busTopology struct {
	nodeID string
}

func newBusTopology(name string) busTopology {
	registry := topology.Current()
	if registry == nil {
		return busTopology{}
	}

	return busTopology{
		nodeID: registry.AddNode(topology.Node{Kind: topology.NodeBus, Name: name}),
	}
}

// publish publishes the message with the publisher.
// The message is marked as published by the bus, unless it's published from a handler.
// If the publisher doesn't report the publish, the bus does it.
func (b busTopology) publish(publisher message.Publisher, topic string, msg *message.Message) error {
	if b.nodeID == "" {
		return publisher.Publish(topic, msg)
	}

	ctx := msg.Context()
	if topology.SourceFromContext(ctx).NodeID == "" {
		ctx = topology.ContextWithSource(ctx, topology.Source{NodeID: b.nodeID})
	}
	ctx, tracker := topology.TrackPublish(ctx)
	msg.SetContext(ctx)

	if err := publisher.Publish(topic, msg); err != nil {
		return err
	}

	if !tracker.Reported() {
		topology.Current().EmitPublished(publisher, topic, msg.Context(), msg.UUID, msg.Metadata, msg.Payload)
	}

	return nil
}
