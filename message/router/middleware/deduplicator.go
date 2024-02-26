package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/adler32"
	"io"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// MessageHasherReadLimitMinimum specifies the least number
// of bytes of a [message.Message] are used for calculating
// their hash values using a [MessageHasher].
const MessageHasherReadLimitMinimum = 64

// MessageHasher returns a short tag that describes
// a message. The tag should be unique per message,
// but avoiding hash collisions entirely is not practical
// for performance reasons. Used for powering [Deduplicator]s.
type MessageHasher func(*message.Message) (string, error)

// NewMessageHasherAdler32 generates message hashes using a fast
// Adler-32 checksum of the [message.Message] body. Read
// limit specifies how many bytes of the message are
// used for calculating the hash.
//
// Lower limit improves performance but results in more false
// positives. Read limit must be greater than
// [MessageHasherReadLimitMinimum].
func NewMessageHasherAdler32(readLimit int64) MessageHasher {
	if readLimit < MessageHasherReadLimitMinimum {
		readLimit = MessageHasherReadLimitMinimum
	}

	return func(m *message.Message) (string, error) {
		h := adler32.New()
		_, err := io.CopyN(h, bytes.NewReader(m.Payload), readLimit)
		if err != nil && err != io.EOF {
			return "", err
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}
}

// NewMessageHasherSHA256 generates message hashes using a slower
// but more resilient hashing of the [message.Message] body. Read
// limit specifies how many bytes of the message are
// used for calculating the hash.
//
// Lower limit improves performance but results in more false
// positives. Read limit must be greater than
// [MessageHasherReadLimitMinimum].
func NewMessageHasherSHA256(readLimit int64) MessageHasher {
	if readLimit < MessageHasherReadLimitMinimum {
		readLimit = MessageHasherReadLimitMinimum
	}

	return func(m *message.Message) (string, error) {
		h := sha256.New()
		_, err := io.CopyN(h, bytes.NewReader(m.Payload), readLimit)
		if err != nil && err != io.EOF {
			return "", err
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}
}

// NewMessageHasherFromMetadataField looks for a hash value
// inside message metadata instead of calculating a new one.
// Useful if a [MessageHasher] was applied in a previous
// [message.HandlerFunc].
func NewMessageHasherFromMetadataField(field string) MessageHasher {
	return func(m *message.Message) (string, error) {
		fromMetadata, ok := m.Metadata[field]
		if ok {
			return fromMetadata, nil
		}
		return "", fmt.Errorf("cannot recover hash value from metadata of message #%s: field %q is absent", m.UUID, field)
	}
}

type Deduplicator struct {
	tagger MessageHasher
	window time.Duration

	mu   *sync.Mutex
	tags map[string]time.Time
}

func (d *Deduplicator) cleanOutLoop(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return // execution ended, part the go routine
		case tagsBefore := <-ticker.C:
			d.cleanOut(tagsBefore)
		}
	}
}

func (d *Deduplicator) cleanOut(tagsBefore time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for hash, expires := range d.tags {
		if expires.Before(tagsBefore) {
			delete(d.tags, hash)
		}
	}
}

// IsDuplicate returns true if the message hash tag calculated
// using a [MessageHasher] was seen in deduplication time window.
func (d *Deduplicator) IsDuplicate(m *message.Message) (bool, error) {
	tag, err := d.tagger(m)
	if err != nil {
		return false, err
	}

	t := time.Now()
	d.mu.Lock()
	_, alreadySeen := d.tags[tag]
	if alreadySeen {
		// NOTE: could also check if tag expires.After(t)
		// and remove it for exact expiration
		// instead of fuzzy until-next clean up expiration
		// but this should not be needed for most use cases.
		d.mu.Unlock()
		return true, nil
	}
	d.tags[tag] = t.Add(d.window)
	d.mu.Unlock()
	return false, nil // first time, not a duplicate
}

// NewDeduplicator returns a new Deduplicator.
// Call [Deduplicator.Middleware] for a new middleware
// or [Deduplicator.Decorator] for a [message.PublisherDecorator].
//
// Use [NewMessageHasherAdler32] for fast tagging or
// [NewMessageHasherSHA256] for minimal collisions.
//
// Window specifies the minimum duration of  how long the
// duplicate tags are remembered for. Real duration can
// extend up to 50% longer because it depends on the
// clean up cycle.
func NewDeduplicator(
	tagger MessageHasher,
	window time.Duration,
) *Deduplicator {
	if tagger == nil {
		panic("cannot use a <nil> tagger")
	}
	if window < time.Millisecond {
		panic("deduplication window of less than a millisecond is impractical")
	}

	d := &Deduplicator{
		tagger: tagger,
		window: window,

		mu:   &sync.Mutex{},
		tags: make(map[string]time.Time),
	}
	go d.cleanOutLoop(context.Background(), time.NewTicker(window/2))
	return d
}

// Middleware returns the [Deduplicator] middleware.
func (d *Deduplicator) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		isDuplicate, err := d.IsDuplicate(msg)
		if err != nil {
			return nil, err
		}
		if isDuplicate {
			return nil, nil
		}
		return h(msg)
	}
}

type deduplicatingPublisherDecorator struct {
	message.Publisher
	deduplicator *Deduplicator
}

func (d *deduplicatingPublisherDecorator) Publish(
	topic string,
	messages ...*message.Message,
) (err error) {
	notRecent := make([]*message.Message, 0, len(messages))
	isDuplicate := false

	for _, m := range messages {
		isDuplicate, err = d.deduplicator.IsDuplicate(m)
		if err != nil {
			return err
		}
		if isDuplicate {
			m.Ack() // acknowledge and ignore
			continue
		}
		notRecent = append(notRecent, m)
	}
	return d.Publisher.Publish(topic, notRecent...)
}

// PublisherDecorator returns a decorator that
// acknowledges and drops every [message.Message] that
// was recognized by a [Deduplicator].
func (d *Deduplicator) PublisherDecorator() message.PublisherDecorator {
	return func(pub message.Publisher) (message.Publisher, error) {
		if pub == nil {
			return nil, errors.New("cannot decorate a <nil> publisher")
		}

		return &deduplicatingPublisherDecorator{
			Publisher:    pub,
			deduplicator: d,
		}, nil
	}
}
