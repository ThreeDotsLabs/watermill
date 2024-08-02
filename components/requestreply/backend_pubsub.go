package requestreply

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// PubSubBackend is a Backend that uses Pub/Sub to transport commands and replies.
type PubSubBackend[Result any] struct {
	config    PubSubBackendConfig
	marshaler BackendPubsubMarshaler[Result]
}

// NewPubSubBackend creates a new PubSubBackend.
//
// If you want to use backend together with `NewCommandHandler` (without result), you should pass `NoResult` or `struct{}` as Result type.
func NewPubSubBackend[Result any](
	config PubSubBackendConfig,
	marshaler BackendPubsubMarshaler[Result],
) (*PubSubBackend[Result], error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if marshaler == nil {
		return nil, errors.New("marshaler cannot be nil")
	}

	return &PubSubBackend[Result]{
		config:    config,
		marshaler: marshaler,
	}, nil
}

type PubSubBackendSubscribeParams struct {
	Command any

	OperationID OperationID
}

type PubSubBackendSubscriberConstructorFn func(PubSubBackendSubscribeParams) (message.Subscriber, error)

type PubSubBackendGenerateSubscribeTopicFn func(PubSubBackendSubscribeParams) (string, error)

type PubSubBackendPublishParams struct {
	Command any

	CommandMessage *message.Message

	OperationID OperationID
}

type PubSubBackendGeneratePublishTopicFn func(PubSubBackendPublishParams) (string, error)

type PubSubBackendOnCommandProcessedParams struct {
	HandleErr error

	PubSubBackendPublishParams
}

type PubSubBackendModifyNotificationMessageFn func(msg *message.Message, params PubSubBackendOnCommandProcessedParams) error

type PubSubBackendOnListenForReplyFinishedFn func(ctx context.Context, params PubSubBackendSubscribeParams)

type ReplyPublishErrorHandler func(replyTopic string, notificationMsg *message.Message, err error) error

type PubSubBackendConfig struct {
	Publisher             message.Publisher
	SubscriberConstructor PubSubBackendSubscriberConstructorFn

	GeneratePublishTopic   PubSubBackendGeneratePublishTopicFn
	GenerateSubscribeTopic PubSubBackendGenerateSubscribeTopicFn

	Logger watermill.LoggerAdapter

	ListenForReplyTimeout *time.Duration

	ModifyNotificationMessage PubSubBackendModifyNotificationMessageFn

	OnListenForReplyFinished PubSubBackendOnListenForReplyFinishedFn

	// AckCommandErrors determines if the command should be acked or nacked when handler returns an error.
	// Command will be nacked by default when sending reply fails, you can control this behaviour with the
	// ReplyPublishErrorHandler config option.
	// You should use this option instead of cqrs.CommandProcessorConfig.AckCommandHandlingErrors, as it's aware
	// if error was returned by handler or sending reply failed.
	AckCommandErrors bool

	// ReplyPublishErrorHandler if not nil will be invoked when sending the reply fails. If it returns an error
	// the command will ba nacked.
	ReplyPublishErrorHandler ReplyPublishErrorHandler
}

func (p *PubSubBackendConfig) setDefaults() {
	if p.Logger == nil {
		p.Logger = watermill.NopLogger{}
	}
}

func (p *PubSubBackendConfig) Validate() error {
	var err error

	if p.Publisher == nil {
		err = errors.Join(err, errors.New("publisher cannot be nil"))
	}
	if p.SubscriberConstructor == nil {
		err = errors.Join(err, errors.New("subscriber constructor cannot be nil"))
	}
	if p.GeneratePublishTopic == nil {
		err = errors.Join(err, errors.New("GeneratePublishTopic cannot be nil"))
	}
	if p.GenerateSubscribeTopic == nil {
		err = errors.Join(err, errors.New("GenerateSubscribeTopic cannot be nil"))
	}

	return err
}

func (p PubSubBackend[Result]) ListenForNotifications(
	ctx context.Context,
	params BackendListenForNotificationsParams,
) (<-chan Reply[Result], error) {
	start := time.Now()

	replyContext := PubSubBackendSubscribeParams(params)

	// this needs to be done before publishing the message to avoid race condition
	notificationsSubscriber, err := p.config.SubscriberConstructor(replyContext)
	if err != nil {
		return nil, fmt.Errorf("cannot create request/reply notifications subscriber: %w", err)
	}

	replyNotificationTopic, err := p.config.GenerateSubscribeTopic(replyContext)
	if err != nil {
		return nil, fmt.Errorf("cannot generate request/reply notifications topic: %w", err)
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
		return nil, fmt.Errorf("cannot subscribe to request/reply notifications topic: %w", err)
	}

	p.config.Logger.Debug(
		"Subscribed to request/reply notifications topic",
		watermill.LogFields{
			"request_reply_topic": replyNotificationTopic,
		},
	)

	replyChan := make(chan Reply[Result], 1)

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
				replyChan <- Reply[Result]{
					Error: ReplyTimeoutError{time.Since(start), ctx.Err()},
				}
				return
			case notifyMsg, ok := <-notifyMsgs:
				if !ok {
					// subscriber is closed
					replyChan <- Reply[Result]{
						Error: ReplyTimeoutError{time.Since(start), fmt.Errorf("subscriber closed")},
					}
					return
				}

				resp, ok, unmarshalErr := p.handleNotifyMsg(notifyMsg, string(params.OperationID), p.marshaler)
				if unmarshalErr != nil {
					replyChan <- Reply[Result]{
						Error: ReplyUnmarshalError{unmarshalErr},
					}
				} else if ok {
					replyChan <- Reply[Result]{
						HandlerResult:       resp.HandlerResult,
						Error:               resp.Error,
						NotificationMessage: notifyMsg,
					}
				}

				// we assume that more messages may arrive (in case of fan-out commands handling) - we don't exit yet
			}
		}
	}()

	return replyChan, nil
}

const OperationIDMetadataKey = "_watermill_requestreply_op_id"

func (p PubSubBackend[Result]) OnCommandProcessed(ctx context.Context, params BackendOnCommandProcessedParams[Result]) error {
	p.config.Logger.Debug("Sending request reply", nil)

	notificationMsg, err := p.marshaler.MarshalReply(params)
	if err != nil {
		return fmt.Errorf("cannot marshal request reply notification: %w", err)
	}
	notificationMsg.SetContext(ctx)

	operationID, err := operationIDFromMetadata(params.CommandMessage)
	if err != nil {
		return err
	}
	notificationMsg.Metadata.Set(OperationIDMetadataKey, string(operationID))

	if p.config.ModifyNotificationMessage != nil {
		processedContext := PubSubBackendOnCommandProcessedParams{
			HandleErr: params.HandleErr,
			PubSubBackendPublishParams: PubSubBackendPublishParams{
				Command:        params.Command,
				CommandMessage: params.CommandMessage,
				OperationID:    operationID,
			},
		}
		if err := p.config.ModifyNotificationMessage(notificationMsg, processedContext); err != nil {
			return fmt.Errorf("cannot modify notification message: %w", err)
		}
	}

	replyTopic, err := p.config.GeneratePublishTopic(PubSubBackendPublishParams{
		Command:        params.Command,
		CommandMessage: params.CommandMessage,
		OperationID:    operationID,
	})
	if err != nil {
		return fmt.Errorf("cannot generate request/reply notify topic: %w", err)
	}

	err = p.config.Publisher.Publish(replyTopic, notificationMsg)
	if err != nil {
		if p.config.ReplyPublishErrorHandler != nil {
			err = p.config.ReplyPublishErrorHandler(replyTopic, notificationMsg, err)
		}
	}
	if err != nil {
		return fmt.Errorf("cannot publish command executed message: %w", err)
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

func operationIDFromMetadata(msg *message.Message) (OperationID, error) {
	operationID := msg.Metadata.Get(OperationIDMetadataKey)
	if operationID == "" {
		return "", fmt.Errorf("cannot get notification ID from command message metadata, key: %s", OperationIDMetadataKey)
	}

	return OperationID(operationID), nil
}

func (p PubSubBackend[Result]) handleNotifyMsg(
	msg *message.Message,
	expectedCommandUuid string,
	marshaler BackendPubsubMarshaler[Result],
) (Reply[Result], bool, error) {
	defer msg.Ack()

	if msg.Metadata.Get(OperationIDMetadataKey) != expectedCommandUuid {
		p.config.Logger.Debug("Received notify message with different command UUID", nil)
		return Reply[Result]{}, false, nil
	}

	res, unmarshalErr := marshaler.UnmarshalReply(msg)
	return res, true, unmarshalErr
}
