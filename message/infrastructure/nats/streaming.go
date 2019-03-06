package nats

import "github.com/nats-io/go-nats-streaming"

type NatsStreamingConfig struct {
	// ClusterID is the NATS Streaming cluster ID.
	ClusterID string

	// ClientID is the NATS Streaming client ID to connect with.
	// ClientID can contain only alphanumeric and `-` or `_` characters.
	//
	// Using DurableName causes the NATS Streaming server to track
	// the last acknowledged message for that ClientID + DurableName.
	ClientID string

	// StanOptions are custom []stan.Option passed to the connection.
	// It is also used to provide connection parameters, for example:
	// 		stan.NatsURL("nats://localhost:4222")
	StanOptions []stan.Option

}

func NewStanConnection(config *NatsStreamingConfig) (stan.Conn,error) {
	return stan.Connect(config.ClusterID, config.ClientID, config.StanOptions...)
}

func NewStanConnectionWithDefaultSubscriberOption(config *NatsStreamingConfig)  (stan.Conn,error) {
	config.StanOptions = append(
		config.StanOptions,
		stan.SetManualAckMode(), // manual AckMode is required to support acking/nacking by client
		stan.AckWait(c.AckWaitTimeout),
	)
	return NewStanConnection(config)
}

