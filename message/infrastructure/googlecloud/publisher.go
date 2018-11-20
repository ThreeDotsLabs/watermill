package googlecloud

import (
	"context"

	"github.com/pkg/errors"

	"cloud.google.com/go/pubsub"
	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/api/option"
)

var (
	ErrTopicDoesNotExist = errors.New("topic does not exist")
	ErrPublisherClosed   = errors.New("publisher is closed")
)

type PublisherOption func(*publisher)

type publisher struct {
	closed bool

	client *pubsub.Client

	clientOptions      []option.ClientOption
	publishSettings    *pubsub.PublishSettings
	projectID          string
	createMissingTopic bool
	marshaler          Marshaler
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

	return p.client.Close()
}

func NewPublisher(ctx context.Context, opts ...PublisherOption) (message.Publisher, error) {
	pub := &publisher{}

	for _, opt := range opts {
		opt(pub)
	}

	if pub.marshaler == nil {
		pub.marshaler = DefaultMarshaler{}
	}

	var err error
	pub.client, err = pubsub.NewClient(ctx, pub.projectID, pub.clientOptions...)
	if err != nil {
		return nil, err
	}

	return pub, nil
}

func ProjectID(projectID string) PublisherOption {
	return func(pub *publisher) {
		pub.projectID = projectID
	}
}

func ClientOptions(opts ...option.ClientOption) PublisherOption {
	return func(pub *publisher) {
		pub.clientOptions = opts
	}
}

func PublishSettings(settings pubsub.PublishSettings) PublisherOption {
	return func(pub *publisher) {
		pub.publishSettings = new(pubsub.PublishSettings)
		*pub.publishSettings = settings
	}
}

func CreateTopicIfMissing() PublisherOption {
	return func(pub *publisher) {
		pub.createMissingTopic = true
	}
}

func WithMarshaler(marshaler Marshaler) PublisherOption {
	return func(pub *publisher) {
		pub.marshaler = marshaler
	}
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
