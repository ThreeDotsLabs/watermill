package cli

import (
	"context"

	"github.com/pkg/errors"
)

type BackendConfig struct {
	Topic    string
	RawTopic string
}

func (c BackendConfig) Validate() error {
	if c.Topic == "" && c.RawTopic == "" {
		return errors.New("topic or raw topic must be provided")
	}

	if c.Topic != "" && c.RawTopic != "" {
		return errors.New("only one of topic or raw topic must be provided")
	}

	return nil
}

type BackendConstructor func(ctx context.Context, cfg BackendConfig) (Backend, error)

type Backend interface {
	AllMessages(ctx context.Context) ([]Message, error)
	Requeue(ctx context.Context, id string) error
	Ack(ctx context.Context, id string) error
}
