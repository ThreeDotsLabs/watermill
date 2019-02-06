package watermill_test

import (
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
)

func testuUniqness(t *testing.T, genFunc func() string) {
	producers := 100
	uuidsPerProducer := 10000

	uuidsCount := producers * uuidsPerProducer

	uuids := make(chan string, uuidsCount)
	allGenerated := sync.WaitGroup{}
	allGenerated.Add(producers)

	for i := 0; i < producers; i++ {
		go func() {
			for j := 0; j < uuidsPerProducer; j++ {
				uuids <- genFunc()
			}
			allGenerated.Done()
		}()
	}

	uniqueUUIDs := make(map[string]struct{}, uuidsCount)

	allGenerated.Wait()
	close(uuids)

	for uuid := range uuids {
		if _, ok := uniqueUUIDs[uuid]; ok {
			t.Error(uuid, " has duplicate")
		}
		uniqueUUIDs[uuid] = struct{}{}
	}
}

func TestUUID(t *testing.T) {
	testuUniqness(t, watermill.UUID)
}

func TestShortUUID(t *testing.T) {
	testuUniqness(t, watermill.ShortUUID)
}

func TestULID(t *testing.T) {
	testuUniqness(t, watermill.ULID)
}
