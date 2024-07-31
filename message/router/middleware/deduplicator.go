package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/adler32"
	"io"
	"math"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// MessageHasherReadLimitMinimum specifies the least number
// of bytes of a [message.Message] are used for calculating
// their hash values using a [MessageHasher].
const MessageHasherReadLimitMinimum = 64

// ExpiringKeyRepository is a state container for checking the
// existence of a key in a certain time window.
// All operations must be safe for concurrent use.
type ExpiringKeyRepository interface {
	// IsDuplicate returns `true` if the key
	// was not checked in recent past.
	// The key must expire in a certain time window.
	IsDuplicate(ctx context.Context, key string) (ok bool, err error)
}

// MessageHasher returns a short tag that describes
// a message. The tag should be unique per message,
// but avoiding hash collisions entirely is not practical
// for performance reasons. Used for powering [Deduplicator]s.
type MessageHasher func(*message.Message) (string, error)

// Deduplicator drops similar messages if they are present
// in a [ExpiringKeyRepository]. The similarity is determined
// by a [MessageHasher]. Time out is applied to repository
// operations using [context.WithTimeout].
//
// Call [Deduplicator.Middleware] for a new middleware
// or [Deduplicator.Decorator] for a [message.PublisherDecorator].
//
// KeyFactory defaults to [NewMessageHasherAdler32] with read
// limit  set to [math.MaxInt64] for fast tagging.
// Use [NewMessageHasherSHA256] for minimal collisions.
//
// Repository defaults to [NewMapExpiringKeyRepository] with one
// minute retention window. This default setting is performant
// but **does not support distributed operations**. If you
// implement a [ExpiringKeyRepository] backed by Redis,
// please submit a pull request.
//
// Timeout defaults to one minute. If lower than
// five milliseconds, it is set to five milliseconds.
//
// [ExpiringKeyRepository] must expire values
// in a certain time window. If there is no expiration, only one
// unique message will be ever delivered as long as the repository
// keeps its state.
type Deduplicator struct {
	KeyFactory MessageHasher
	Repository ExpiringKeyRepository
	Timeout    time.Duration
}

// IsDuplicate returns true if the message hash tag calculated
// using a [MessageHasher] was seen in deduplication time window.
func (d *Deduplicator) IsDuplicate(m *message.Message) (bool, error) {
	key, err := d.KeyFactory(m)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(m.Context(), d.Timeout)
	defer cancel()
	return d.Repository.IsDuplicate(ctx, key)
}

func applyDefaultsToDeduplicator(d *Deduplicator) *Deduplicator {
	if d == nil {
		kr, err := NewMapExpiringKeyRepository(time.Minute)
		if err != nil {
			panic(err)
		}
		return &Deduplicator{
			KeyFactory: NewMessageHasherAdler32(math.MaxInt64),
			Repository: kr,
			Timeout:    time.Minute,
		}
	}
	if d.KeyFactory == nil {
		d.KeyFactory = NewMessageHasherAdler32(math.MaxInt64)
	}
	if d.Repository == nil {
		kr, err := NewMapExpiringKeyRepository(time.Minute)
		if err != nil {
			panic(err)
		}
		d.Repository = kr
	}
	if d.Timeout < time.Millisecond*5 {
		d.Timeout = time.Millisecond * 5
	}
	return d
}

// Middleware returns the [message.HandlerMiddleware]
// that drops similar messages in a given time window.
func (d *Deduplicator) Middleware(h message.HandlerFunc) message.HandlerFunc {
	d = applyDefaultsToDeduplicator(d)
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

type mapExpiringKeyRepository struct {
	window time.Duration
	mu     *sync.Mutex
	tags   map[string]time.Time
}

// NewMapExpiringKeyRepository returns a memory store
// backed by a regular hash map protected by
// a [sync.Mutex]. The state **cannot be shared or synchronized
// between instances** by design for performance.
//
// If you need to drop duplicate messages by orchestration,
// implement [ExpiringKeyRepository] interface backed by Redis
// or similar.
//
// Window specifies the minimum duration of how long the
// duplicate tags are remembered for. Real duration can
// extend up to 50% longer because it depends on the
// clean up cycle.
func NewMapExpiringKeyRepository(window time.Duration) (ExpiringKeyRepository, error) {
	if window < time.Millisecond {
		return nil, errors.New("deduplication window of less than a millisecond is impractical")
	}

	kr := &mapExpiringKeyRepository{
		window: window,
		mu:     &sync.Mutex{},
		tags:   make(map[string]time.Time),
	}
	go kr.cleanOutLoop(context.Background(), time.NewTicker(window/2))
	return kr, nil
}

func (kr *mapExpiringKeyRepository) IsDuplicate(
	ctx context.Context,
	key string,
) (bool, error) {
	kr.mu.Lock()
	_, alreadySeen := kr.tags[key]
	if alreadySeen {
		// NOTE: could also check if key expires.After(t)
		// and remove it for exact expiration
		// instead of fuzzy until-next clean up expiration
		// but this should not be needed for most use cases.
		kr.mu.Unlock()
		return true, nil
	}
	kr.tags[key] = time.Now().Add(kr.window)
	kr.mu.Unlock()
	return false, nil
}

func (kr *mapExpiringKeyRepository) cleanOutLoop(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return // execution ended, part the go routine
		case tagsBefore := <-ticker.C:
			kr.cleanOut(tagsBefore)
		}
	}
}

func (kr *mapExpiringKeyRepository) cleanOut(tagsBefore time.Time) {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	for hash, expires := range kr.tags {
		if expires.Before(tagsBefore) {
			delete(kr.tags, hash)
		}
	}
}

// Len returns the number of known tags that have not been
// cleaned out yet.
func (kr *mapExpiringKeyRepository) Len() (count int) {
	kr.mu.Lock()
	count = len(kr.tags)
	kr.mu.Unlock()
	return
}

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
		return string(h.Sum(nil)), nil
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
		return string(h.Sum(nil)), nil
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
//
// The returned decorator provides the same functionality
// to a [message.Publisher] as [Deduplicator.Middleware]
// to a [message.Router].
func (d *Deduplicator) PublisherDecorator() message.PublisherDecorator {
	return func(pub message.Publisher) (message.Publisher, error) {
		if pub == nil {
			return nil, errors.New("cannot decorate a <nil> publisher")
		}

		return &deduplicatingPublisherDecorator{
			Publisher:    pub,
			deduplicator: applyDefaultsToDeduplicator(d),
		}, nil
	}
}
