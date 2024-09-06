package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	RequeueTimeMetadataKey  = "requeue_time"
	RequeueDelayMetadataKey = "requeue_delay"
)

type DelayMetadata struct {
	config DelayMetadataConfig
}

type DelayMetadataConfig struct {
	Delay    time.Duration
	MaxDelay time.Duration
}

func (c *DelayMetadataConfig) setDefaults() {
	if c.Delay == 0 {
		c.Delay = 10 * time.Second
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 160 * time.Second
	}
}

func NewDelayMetadata(config DelayMetadataConfig) *DelayMetadata {
	config.setDefaults()

	return &DelayMetadata{
		config: config,
	}
}

func (d *DelayMetadata) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := h(msg)
		if err != nil {
			d.applyDelay(msg)
		}

		return msgs, err
	}
}

func (d *DelayMetadata) applyDelay(msg *message.Message) {
	currentDelayStr := msg.Metadata.Get(RequeueDelayMetadataKey)
	currentDelay, err := time.ParseDuration(currentDelayStr)
	if err != nil {
		currentDelay = d.config.Delay
	} else {
		currentDelay = currentDelay * 2
	}

	requeueTime := time.Now().UTC().Add(currentDelay)
	msg.Metadata.Set(RequeueTimeMetadataKey, requeueTime.Format(time.RFC3339))
	msg.Metadata.Set(RequeueDelayMetadataKey, currentDelay.String())
}
