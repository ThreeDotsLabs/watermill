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
	ErrTopicDoesNotExist = errors.New("topic does not exist")
	ErrPublisherClosed   = errors.New("publisher is closed")
)

type publisher struct {
	ctx context.Context

	topics     map[string]*pubsub.Topic
	topicsLock sync.RWMutex
	closed     bool

	client *pubsub.Client
	config PublisherConfig
}

type PublisherConfig struct {
	ClientOptions           []option.ClientOption
	PublishSettings         *pubsub.PublishSettings
	ProjectID               string
	DoNotCreateMissingTopic bool
	Marshaler               Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshalerUnmarshaler{}
	}
}

func (c PublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return errors.New("empty googlecloud message marshaler")
	}

	return nil
}

func NewPublisher(ctx context.Context, config PublisherConfig) (message.Publisher, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	pub := &publisher{
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

func (p *publisher) Publish(topic string, messages ...*message.Message) error {
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

func (p *publisher) Close() error {
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

func (p *publisher) topic(ctx context.Context, topic string) (t *pubsub.Topic, err error) {
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

	if p.config.DoNotCreateMissingTopic {
		return nil, errors.Wrap(ErrTopicDoesNotExist, topic)
	}

	t, err = p.client.CreateTopic(ctx, topic)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create topic %s", topic)
	}

	return t, nil
}
