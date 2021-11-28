package watermill

import (
	"crypto/rand"

	"github.com/google/uuid"
	"github.com/lithammer/shortuuid/v3"
	"github.com/oklog/ulid"
)

// NewUUID returns a new UUID Version 4.
func NewUUID() string {
	return uuid.New().String()
}

// NewShortUUID returns a new short UUID.
func NewShortUUID() string {
	return shortuuid.New()
}

// NewULID returns a new ULID.
func NewULID() string {
	return ulid.MustNew(ulid.Now(), rand.Reader).String()
}
