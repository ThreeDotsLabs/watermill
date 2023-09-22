package requestreply

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// PubSubBackend is a Backend that uses Pub/Sub to transport commands and replies.
type PubSubBackend[Response any] struct {
	config    PubSubBackendConfig
	marshaler BackendPubsubMarshaler[Response]
}

// NewPubSubBackend creates a new PubSubBackend.
// todo: doc usage
func NewPubSubBackend[Response any](
	config PubSubBackendConfig,
	marshaler BackendPubsubMarshaler[Response],
) (*PubSubBackend[Response], error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	if marshaler == nil {
		return nil, errors.New("marshaler cannot be nil")
	}

	return &PubSubBackend[Response]{
		config:    config,
		marshaler: marshaler,
	}, nil
}

type PubSubBackendSubscribeParams struct {
	Command any

	// todo: doc everywhere
	NotificationID string
}

type PubSubBackendSubscriberConstructorFn func(PubSubBackendSubscribeParams) (message.Subscriber, error)

type PubSubBackendGenerateSubscribeTopicFn func(PubSubBackendSubscribeParams) (string, error)

type PubSubBackendPublishParams struct {
	Command any

	CommandMessage *message.Message

	// todo: doc everywhere
	NotificationID string
}

type PubSubBackendGeneratePublishTopicFn func(PubSubBackendPublishParams) (string, error)

type PubSubBackendOnCommandProcessedParams struct {
	HandleErr error

	PubSubBackendPublishParams
}

type PubSubBackendModifyNotificationMessageFn func(msg *message.Message, params PubSubBackendOnCommandProcessedParams) error

type PubSubBackendOnListenForReplyFinishedFn func(ctx context.Context, params PubSubBackendSubscribeParams)

type PubSubBackendConfig struct {
	Publisher             message.Publisher
	SubscriberConstructor PubSubBackendSubscriberConstructorFn

	GeneratePublishTopic   PubSubBackendGeneratePublishTopicFn
	GenerateSubscribeTopic PubSubBackendGenerateSubscribeTopicFn

	Logger watermill.LoggerAdapter

	ListenForReplyTimeout *time.Duration

	ModifyNotificationMessage PubSubBackendModifyNotificationMessageFn

	OnListenForReplyFinished PubSubBackendOnListenForReplyFinishedFn

	AckCommandErrors bool
}

func (p *PubSubBackendConfig) setDefaults() {
	if p.Logger == nil {
		p.Logger = watermill.NopLogger{}
	}
}

func (p *PubSubBackendConfig) Validate() error {
	if p.Publisher == nil {
		return errors.New("publisher cannot be nil")
	}
	if p.SubscriberConstructor == nil {
		return errors.New("subscriber constructor cannot be nil")
	}
	if p.GeneratePublishTopic == nil {
		return errors.New("GeneratePublishTopic cannot be nil")
	}
	if p.GenerateSubscribeTopic == nil {
		return errors.New("GenerateSubscribeTopic cannot be nil")
	}

	return nil
}

func (p PubSubBackend[Response]) ListenForNotifications(
	ctx context.Context,
	params BackendListenForNotificationsParams,
) (<-chan CommandReply[Response], error) {
	start := time.Now()

	replyContext := PubSubBackendSubscribeParams{
		Command:        params.Command,
		NotificationID: params.NotificationID,
	}

	// this needs to be done before publishing the message to avoid race condition
	notificationsSubscriber, err := p.config.SubscriberConstructor(replyContext)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create request/reply notifications subscriber")
	}

	replyNotificationTopic, err := p.config.GenerateSubscribeTopic(replyContext)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate request/reply notifications topic")
	}

	var cancel context.CancelFunc
	if p.config.ListenForReplyTimeout != nil {
		ctx, cancel = context.WithTimeout(ctx, *p.config.ListenForReplyTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	notifyMsgs, err := notificationsSubscriber.Subscribe(ctx, replyNotificationTopic)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "cannot subscribe to request/reply notifications topic")
	}

	p.config.Logger.Debug(
		"Subscribed to request/reply notifications topic",
		watermill.LogFields{
			"request_reply_topic": replyNotificationTopic,
		},
	)

	replyChan := make(chan CommandReply[Response], 1)

	go func() {
		defer func() {
			if p.config.OnListenForReplyFinished == nil {
				return
			}

			p.config.OnListenForReplyFinished(ctx, replyContext)
		}()
		defer close(replyChan)
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				replyChan <- CommandReply[Response]{
					HandlerErr: ReplyTimeoutError{time.Since(start), ctx.Err()},
				}
				return
			case notifyMsg, ok := <-notifyMsgs:
				if !ok {
					// subscriber is closed
					replyChan <- CommandReply[Response]{
						HandlerErr: ReplyTimeoutError{time.Since(start), fmt.Errorf("subscriber closed")},
					}
					return
				}

				resp, ok, unmarshalErr := p.handleNotifyMsg(notifyMsg, params.NotificationID, p.marshaler)
				if unmarshalErr != nil {
					replyChan <- CommandReply[Response]{
						HandlerErr: ReplyUnmarshalError{unmarshalErr},
					}
				} else if ok {
					replyChan <- CommandReply[Response]{
						HandlerResponse: resp.HandlerResponse,
						HandlerErr:      resp.HandlerErr,
						ReplyMsg:        notifyMsg,
					}
				}

				// we assume that more messages may arrive (in case of fan-out commands handling) - we don't exit yet
			}
		}
	}()

	return replyChan, nil
}

const NotificationIdMetadataKey = "_watermill_command_message_uuid"

func (p PubSubBackend[Response]) OnCommandProcessed(ctx context.Context, params BackendOnCommandProcessedParams[Response]) error {
	p.config.Logger.Debug("Sending request reply", nil)

	notificationMsg, err := p.marshaler.MarshalReply(params)
	if err != nil {
		return errors.Wrap(err, "cannot marshal request reply notification")
	}
	notificationMsg.SetContext(ctx)

	notificationID := params.CommandMessage.Metadata.Get(NotificationIdMetadataKey)
	if notificationID == "" {
		return errors.Errorf("cannot get notification ID from command message metadata, key: %s", NotificationIdMetadataKey)
	}
	notificationMsg.Metadata.Set(NotificationIdMetadataKey, notificationID)

	if p.config.ModifyNotificationMessage != nil {
		processedContext := PubSubBackendOnCommandProcessedParams{
			HandleErr: params.HandleErr,
			PubSubBackendPublishParams: PubSubBackendPublishParams{
				Command:        params.Command,
				CommandMessage: params.CommandMessage,
				NotificationID: notificationID,
			},
		}
		if err := p.config.ModifyNotificationMessage(notificationMsg, processedContext); err != nil {
			return errors.Wrap(err, "cannot modify notification message")
		}
	}

	replyTopic, err := p.config.GeneratePublishTopic(PubSubBackendPublishParams{
		Command:        params.Command,
		CommandMessage: params.CommandMessage,
		NotificationID: notificationID,
	})
	if err != nil {
		return errors.Wrap(err, "cannot generate request/reply notify topic")
	}

	if err := p.config.Publisher.Publish(replyTopic, notificationMsg); err != nil {
		return errors.Wrap(err, "cannot publish command executed message")
	}

	if p.config.AckCommandErrors {
		// we are ignoring handler error - message will be acked
		return nil
	} else {
		// if handler returned error, it will nack the message
		// if params.HandleErr is nil, message will be acked
		return params.HandleErr
	}
}

func (p PubSubBackend[Response]) handleNotifyMsg(
	msg *message.Message,
	expectedCommandUuid string,
	marshaler BackendPubsubMarshaler[Response],
) (CommandReply[Response], bool, error) {
	defer msg.Ack()

	if msg.Metadata.Get(NotificationIdMetadataKey) != expectedCommandUuid {
		p.config.Logger.Debug("Received notify message with different command UUID", nil)
		return CommandReply[Response]{}, false, nil
	}

	res, unmarshalErr := marshaler.UnmarshalReply(msg)
	return res, true, unmarshalErr
}
