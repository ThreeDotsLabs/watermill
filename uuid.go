package watermill

import (
	"crypto/rand"

	"github.com/google/uuid"
	"github.com/lithammer/shortuuid/v3"
	"github.com/oklog/ulid"
)

func NewUUID() string {
	return uuid.New().String()
}

func NewShortUUID() string {
	return shortuuid.New()
}

func NewULID() string {
	return ulid.MustNew(ulid.Now(), rand.Reader).String()
}
