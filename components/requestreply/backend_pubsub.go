package requestreply

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// PubSubRequestReply is a Backend that uses Pub/Sub to transport commands and replies.
type PubSubRequestReply[Response any] struct {
	config    PubSubRequestReplyConfig
	marshaler Marshaler[Response]
}

// NewPubSubRequestReply creates a new PubSubRequestReply.
func NewPubSubRequestReply[Response any](
	config PubSubRequestReplyConfig,
	marshaler Marshaler[Response],
) (*PubSubRequestReply[Response], error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	if marshaler == nil {
		return nil, errors.New("marshaler cannot be nil")
	}

	return &PubSubRequestReply[Response]{
		config:    config,
		marshaler: marshaler,
	}, nil
}

type PubSubRequestReplySubscriberContext struct {
	Command any
}

type PubSubRequestReplyOnCommandProcessedContext struct {
	HandleErr error

	PubSubRequestReplySubscriberContext
}

type PubSubRequestReplySubscriberConstructorFn func(PubSubRequestReplySubscriberContext) (message.Subscriber, error)

type PubSubRequestReplyTopicGeneratorFn func(PubSubRequestReplySubscriberContext) (string, error)

type PubSubRequestReplyConfig struct {
	Publisher                      message.Publisher
	SubscriberConstructor          PubSubRequestReplySubscriberConstructorFn
	GenerateReplyNotificationTopic PubSubRequestReplyTopicGeneratorFn

	Logger watermill.LoggerAdapter

	ListenForReplyTimeout *time.Duration

	ModifyNotificationMessage func(msg *message.Message, context PubSubRequestReplyOnCommandProcessedContext) error

	OnListenForReplyFinished func(context.Context, PubSubRequestReplySubscriberContext)
}

func (p *PubSubRequestReplyConfig) setDefaults() {
	if p.Logger == nil {
		p.Logger = watermill.NopLogger{}
	}
}

func (p *PubSubRequestReplyConfig) Validate() error {
	if p.Publisher == nil {
		return errors.New("publisher cannot be nil")
	}
	if p.SubscriberConstructor == nil {
		return errors.New("subscriber constructor cannot be nil")
	}
	if p.GenerateReplyNotificationTopic == nil {
		return errors.New("GenerateReplyNotificationTopic cannot be nil")
	}

	return nil
}

const notifyWhenExecutedMetadataKey = "_watermill_notify_when_executed"

//func (p PubSubRequestReply) ModifyCommandMessageBeforePublish(cmdMsg *message.Message, command any) error {
//	cmdMsg.Metadata.Set(notifyWhenExecutedMetadataKey, "1")
//
//	return nil
//}

func (p PubSubRequestReply[Response]) ListenForNotifications(
	ctx context.Context,
	params ListenForNotificationsParams,
) (<-chan CommandReply[Response], error) {
	start := time.Now()

	replyContext := PubSubRequestReplySubscriberContext{
		Command: params.Command,
	}

	// this needs to be done before publishing the message to avoid race condition
	notificationsSubscriber, err := p.config.SubscriberConstructor(replyContext)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create request/reply notifications subscriber")
	}

	replyNotificationTopic, err := p.config.GenerateReplyNotificationTopic(replyContext)
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

	//replyChan := make(chan CommandReply[Response], 1)
	//
	//go func() {
	//	defer close(replyChan)
	//
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			return
	//		case notificationMsg, ok := <-notificationMsgsChannel:
	//			if !ok {
	//				return
	//			}
	//
	//			reply, err := marshaler.UnmarshalReply(notificationMsg)
	//			if err != nil {
	//				// todo: doc it
	//				replyChan <- CommandReply[Response]{
	//					Err:      errors.Wrap(err, "cannot unmarshal reply"),
	//					ReplyMsg: notificationMsg,
	//				}
	//			} else {
	//				replyChan <- reply
	//			}
	//		}
	//	}
	//}()

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
					// todo!!! log?
				} else if ok {
					reply := CommandReply[Response]{
						HandlerResponse: resp.HandlerResponse,
						HandlerErr:      resp.HandlerErr,
						ReplyMsg:        notifyMsg,
					}

					replyChan <- reply
					// we assume that more messages may arrive (in case of fan-out commands handling)
				}
			}
		}
	}()

	return replyChan, nil
}

const NotificationIdMetadataKey = "_watermill_command_message_uuid"

func (p PubSubRequestReply[Response]) OnCommandProcessed(ctx context.Context, params OnCommandProcessedParams[Response]) error {
	//if !p.isRequestReplyEnabled(params.CmdMsg) {
	//	p.config.Logger.Debug(fmt.Sprintf("RequestReply is enabled, but %s is missing", notifyWhenExecutedMetadataKey), nil)
	//	return nil
	//}

	p.config.Logger.Debug("Sending request reply", nil)

	//// we are using protobuf message, so it will work both with proto and json marshaler
	//notification := &RequestReplyNotification{}
	//if params.HandleErr != nil {
	//	notification.Error = params.HandleErr.Error()
	//	notification.HasError = true
	//}

	notificationMsg, err := p.marshaler.MarshalReply(
		params,
		CommandReply[Response]{
			HandlerResponse: params.HandlerResponse,
			HandlerErr:      params.HandleErr,
			ReplyMsg:        nil,
		},
	)
	if err != nil {
		return errors.Wrap(err, "cannot marshal request reply notification")
	}

	// todo: doc - why needed?
	notificationMsg.SetContext(ctx)
	// todo: doc
	notificationMsg.Metadata.Set(NotificationIdMetadataKey, params.CmdMsg.Metadata.Get(NotificationIdMetadataKey))

	if p.config.ModifyNotificationMessage != nil {
		processedContext := PubSubRequestReplyOnCommandProcessedContext{
			HandleErr: params.HandleErr,
			PubSubRequestReplySubscriberContext: PubSubRequestReplySubscriberContext{
				Command: params.Cmd,
			},
		}
		if err := p.config.ModifyNotificationMessage(notificationMsg, processedContext); err != nil {
			return errors.Wrap(err, "cannot modify notification message")
		}
	}

	replyTopic, err := p.config.GenerateReplyNotificationTopic(PubSubRequestReplySubscriberContext{
		Command: params.Cmd,
	})
	if err != nil {
		return errors.Wrap(err, "cannot generate request/reply notify topic")
	}

	if err := p.config.Publisher.Publish(replyTopic, notificationMsg); err != nil {
		return errors.Wrap(err, "cannot publish command executed message")
	}

	return nil
}

func (p PubSubRequestReply[Response]) isRequestReplyEnabled(cmdMsg *message.Message) bool {
	notificationEnabled := cmdMsg.Metadata.Get(notifyWhenExecutedMetadataKey)
	enabled, _ := strconv.ParseBool(notificationEnabled)

	return enabled
}

func (p PubSubRequestReply[Response]) handleNotifyMsg(
	msg *message.Message,
	expectedCommandUuid string,
	marshaler Marshaler[Response],
) (CommandReply[Response], bool, error) {
	defer msg.Ack()

	if msg.Metadata.Get(NotificationIdMetadataKey) != expectedCommandUuid {
		// todo: test
		p.config.Logger.Debug("Received notify message with different command UUID", nil)
		return CommandReply[Response]{}, false, nil
	}

	res, unmarshalErr := marshaler.UnmarshalReply(msg)
	return res, true, unmarshalErr
}

//// todo: make it configurable?
//var defaultPubSubRequestReplyMarshalerMarshaler = JSONMarshaler{}
//
//// todo: doc
//type DefaultPubSubRequestReplyMarshaler[Response any] struct{}
//
//func (d DefaultPubSubRequestReplyMarshaler[Response]) Marshal(params OnCommandProcessedParams[Response], response any) (*message.Message, error) {
//	payload, err := defaultPubSubRequestReplyMarshalerMarshaler.Marshal(response)
//	if err != nil {
//		return nil, errors.Wrap(err, "DefaultPubSubRequestReplyMarshaler: cannot marshal response")
//	}
//
//	msg := message.NewMessage(watermill.NewUUID(), payload)
//	msg.Metadata.Set(ErrorMetadataKey, params.HandleErr.Error())
//	if params.HandleErr != nil {
//		msg.Metadata.Set(HasErrorMetadataKey, "1")
//	} else {
//		msg.Metadata.Set(HasErrorMetadataKey, "0")
//	}
//
//	return msg, nil
//}
//
//func (d DefaultPubSubRequestReplyMarshaler) Unmarshal(msg *message.Message) (reply CommandReply, err error) {
//
//}
