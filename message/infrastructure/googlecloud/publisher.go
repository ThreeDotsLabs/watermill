package googlecloud

import (
	"context"

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
	closed bool

	client *pubsub.Client

	publishSettings    *pubsub.PublishSettings
	createMissingTopic bool
	marshaler          Marshaler
}

type PublisherConfig struct {
	ClientOptions      []option.ClientOption
	PublishSettings    *pubsub.PublishSettings
	ProjectID          string
	CreateMissingTopic bool
	Marshaler          Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
}

func (c PublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return errors.New("empty googlecloud message marshaler")
	}

	return nil
}

func (p *publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	// todo: pass context somehow?
	ctx := context.Background()

	t, err := p.topic(ctx, topic)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		googlecloudMsg, err := p.marshaler.Marshal(topic, msg)
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

	return p.client.Close()
}

func NewPublisher(ctx context.Context, config PublisherConfig) (message.Publisher, error) {
	config.setDefaults()
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	pub := &publisher{
		publishSettings:    config.PublishSettings,
		createMissingTopic: config.CreateMissingTopic,
		marshaler:          config.Marshaler,
	}

	var err error
	pub.client, err = pubsub.NewClient(ctx, config.ProjectID, config.ClientOptions...)
	if err != nil {
		return nil, err
	}

	return pub, nil
}

func (p *publisher) topic(ctx context.Context, topic string) (*pubsub.Topic, error) {
	t := p.client.Topic(topic)
	exists, err := t.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists && !p.createMissingTopic {
		return nil, errors.Wrap(ErrTopicDoesNotExist, topic)
	} else if !exists {
		t, err = p.client.CreateTopic(ctx, topic)
		if err != nil {
			return nil, err
		}
	}

	if p.publishSettings != nil {
		t.PublishSettings = *p.publishSettings
	}

	return t, nil
}
