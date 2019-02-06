package watermill

import (
	"crypto/rand"

	"github.com/google/uuid"
	"github.com/oklog/ulid"
	"github.com/renstrom/shortuuid"
)

func UUID() string {
	return uuid.New().String()
}

func ShortUUID() string {
	return shortuuid.New()
}

func ULID() string {
	return ulid.MustNew(ulid.Now(), rand.Reader).String()
}
