package googlecloud

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"google.golang.org/api/option"

	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	// ErrPublisherClosed happens when trying to publish to a topic while the publisher is closed or closing.
	ErrPublisherClosed = errors.New("publisher is closed")
	// ErrTopicDoesNotExist happens when trying to publish or subscribe to a topic that doesn't exist.
	ErrTopicDoesNotExist = errors.New("topic does not exist")
)

type Publisher struct {
	ctx context.Context

	topics     map[string]*pubsub.Topic
	topicsLock sync.RWMutex
	closed     bool

	client *pubsub.Client
	config PublisherConfig
}

type PublisherConfig struct {
	// ProjectID is the Google Cloud Engine project ID.
	ProjectID string

	// If false (default), `Publisher` tries to create a topic if there is none with the requested name.
	// Otherwise, trying to subscribe to non-existent subscription results in `ErrTopicDoesNotExist`.
	DoNotCreateTopicIfMissing bool

	// Settings for cloud.google.com/go/pubsub client library.
	PublishSettings *pubsub.PublishSettings
	ClientOptions   []option.ClientOption

	Marshaler Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshalerUnmarshaler{}
	}
}

func NewPublisher(ctx context.Context, config PublisherConfig) (*Publisher, error) {
	config.setDefaults()

	pub := &Publisher{
		ctx:    ctx,
		topics: map[string]*pubsub.Topic{},
		config: config,
	}

	var err error
	pub.client, err = pubsub.NewClient(ctx, config.ProjectID, config.ClientOptions...)
	if err != nil {
		return nil, err
	}

	return pub, nil
}

// Publish publishes a set of messages on a Google Cloud Pub/Sub topic.
// It blocks until all the messages are successfully published or an error occurred.
//
// To receive messages published to a topic, you must create a subscription to that topic.
// Only messages published to the topic after the subscription is created are available to subscriber applications.
//
// See https://cloud.google.com/pubsub/docs/publisher to find out more about how Google Cloud Pub/Sub Publishers work.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	ctx := p.ctx

	t, err := p.topic(ctx, topic)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		googlecloudMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		result := t.Publish(ctx, googlecloudMsg)
		<-result.Ready()

		_, err = result.Get(ctx)
		if err != nil {
			return errors.Wrapf(err, "publishing message %s failed", msg.UUID)
		}
	}

	return nil
}

// Close notifies the Publisher to stop processing messages, send all the remaining messages and close the connection.
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	p.topicsLock.Lock()
	for _, t := range p.topics {
		t.Stop()
	}
	p.topicsLock.Unlock()

	return p.client.Close()
}

func (p *Publisher) topic(ctx context.Context, topic string) (t *pubsub.Topic, err error) {
	p.topicsLock.RLock()
	t, ok := p.topics[topic]
	p.topicsLock.RUnlock()
	if ok {
		return t, nil
	}

	p.topicsLock.Lock()
	defer func() {
		p.topicsLock.Unlock()
		if err == nil {
			p.topics[topic] = t
		}
	}()

	t = p.client.Topic(topic)

	// todo: theoretically, one could want different publish settings per topic, which is supported by the client lib
	// different instances of publisher may be used then
	if p.config.PublishSettings != nil {
		t.PublishSettings = *p.config.PublishSettings
	}

	exists, err := t.Exists(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "could not check if topic %s exists", topic)
	}

	if exists {
		return t, nil
	}

	if p.config.DoNotCreateTopicIfMissing {
		return nil, errors.Wrap(ErrTopicDoesNotExist, topic)
	}

	t, err = p.client.CreateTopic(ctx, topic)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create topic %s", topic)
	}

	return t, nil
}
