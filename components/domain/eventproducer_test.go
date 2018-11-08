package domain

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/domainents/domain"
	"github.com/stretchr/testify/assert"
)

type testEvent struct {
	Name string
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
