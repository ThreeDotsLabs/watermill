package http

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	// ErrPublisherClosed happens when trying to publish to a topic while the publisher is closed or closing.
	ErrPublisherClosed = errors.New("publisher is closed")
	ErrNoMarshalFunc   = errors.New("marshal function is missing")
	ErrErrorResponse   = errors.New("server responded with error status")
)

// MarshalMessageFunc transforms the message into a HTTP request to be sent to the specified url.
type MarshalMessageFunc func(url string, msg *message.Message) (*http.Request, error)

// DefaultMarshalMessageFunc transforms the message into a HTTP POST request.
// It encodes the UUID and Metadata in request headers.
func DefaultMarshalMessageFunc(url string, msg *message.Message) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(msg.Payload))
	if err != nil {
		return nil, err
	}

	req.Header.Set(HeaderUUID, msg.UUID)

	metadataJson, err := json.Marshal(msg.Metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal metadata to JSON")
	}
	req.Header.Set(HeaderMetadata, string(metadataJson))
	return req, nil
}

type PublisherConfig struct {
	MarshalMessageFunc MarshalMessageFunc
	Client             *http.Client
	// if false (default), when server responds with error (>=400) to the webhook request, the response body is logged.
	DoNotLogResponseBodyOnServerError bool
}

func (c *PublisherConfig) setDefaults() {
	if c.Client == nil {
		c.Client = http.DefaultClient
	}
}

func (c PublisherConfig) validate() error {
	if c.MarshalMessageFunc == nil {
		return ErrNoMarshalFunc
	}

	return nil
}

type Publisher struct {
	logger watermill.LoggerAdapter
	config PublisherConfig

	closed bool
}

// NewPublisher creates a new Publisher.
// It publishes the received messages as HTTP requests.
// The URL, method and payload of the request are determined by the configured MarshalMessageFunc.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid Publisher config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{
		config: config,
		logger: logger,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	for _, msg := range messages {
		req, err := p.config.MarshalMessageFunc(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		logFields := watermill.LogFields{
			"uuid":     msg.UUID,
			"url":      req.URL.String(),
			"method":   req.Method,
			"provider": ProviderName,
		}

		p.logger.Trace("Publishing message", logFields)

		resp, err := p.config.Client.Do(req)
		if err != nil {
			return errors.Wrapf(err, "publishing message %s failed", msg.UUID)
		}

		if err = p.handleResponseBody(resp, logFields); err != nil {
			return err
		}

		if resp.StatusCode >= http.StatusBadRequest {
			return errors.Wrap(ErrErrorResponse, resp.Status)
		}

		if err != nil {
			return errors.Wrapf(err, "could not close response body for message %s", msg.UUID)
		}

		p.logger.Trace("Message published", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true
	return nil
}

func (p Publisher) handleResponseBody(resp *http.Response, logFields watermill.LogFields) error {
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusBadRequest {
		return nil
	}

	if p.config.DoNotLogResponseBodyOnServerError {
		return nil
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "could not read response body")
	}

	logFields = logFields.Add(watermill.LogFields{
		"http_status":   resp.StatusCode,
		"http_response": string(body),
	})
	p.logger.Info("Server responded with error", logFields)

	return nil
}
