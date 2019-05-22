package nats

import stan "github.com/nats-io/stan.go"

type StanConnConfig struct {
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

func NewStanConnection(config *StanConnConfig) (stan.Conn, error) {
	return stan.Connect(config.ClusterID, config.ClientID, config.StanOptions...)
}
