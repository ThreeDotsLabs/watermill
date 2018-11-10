package domain_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/domain"
	"github.com/stretchr/testify/assert"
)

type testEvent struct {
	name string
}

func (testEvent) OccurredOn() time.Time {
	return time.Now()
}

func (t testEvent) Name() string {
	return t.name
}

func (testEvent) AggregateID() []byte {
	return []byte("1")
}

func (testEvent) AggregateType() string {
	return "aggregate_type"
}

func TestEventProducer(t *testing.T) {
	ep := domain.EventProducer{}

	event1 := testEvent{"foo"}
	event2 := testEvent{"bar"}

	ep.RecordThat(event1)
	ep.RecordThat(event2)

	events := ep.PopEvents()

	assert.Equal(t, []domain.Event{event1, event2}, events)

	assert.Empty(t, ep.PopEvents())
}
