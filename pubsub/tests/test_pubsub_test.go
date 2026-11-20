package tests_test

// Self-tests of the conformance suite: each suite check must pass for a correct Pub/Sub
// and fail for a Pub/Sub that violates exactly what the check is about.

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

var suiteChecks = map[string]struct {
	suiteTest       func(*testing.T, tests.TestContext, tests.PubSubConstructor)
	features        tests.Features
	defect          defect
	expectedFailure string
}{
	"subscribe_ctx_values": {
		suiteTest:       tests.TestSubscribeCtx,
		features:        tests.Features{RequireSingleInstance: true},
		defect:          dropsSubscribeCtxValues,
		expectedFailure: "does not carry the values of the context passed to Subscribe",
	},
	"exactly_once_delivery": {
		suiteTest:       tests.TestPublishSubscribe,
		features:        tests.Features{ExactlyOnceDelivery: true, RequireSingleInstance: true},
		defect:          redeliversAfterCatchingUp,
		expectedFailure: "ExactlyOnceDelivery is declared, but 1 more messages were delivered",
	},
	"total_order": {
		suiteTest:       tests.TestPublishSubscribeInOrder,
		features:        tests.Features{GuaranteedOrder: true, GuaranteedTotalOrder: true, RequireSingleInstance: true, ForceShort: true},
		defect:          swapsNeighbours,
		expectedFailure: "messages were not received in the order they were published",
	},
}

const brokenPubSubEnv = "WATERMILL_TESTS_BROKEN_PUBSUB"

func TestSuiteChecks(t *testing.T) {
	for name, check := range suiteChecks {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Run("correct_pubsub_passes", func(t *testing.T) {
				t.Parallel()
				runSuiteTest(t, check.suiteTest, check.features, noDefect)
			})

			// A failing suite test would fail this test too, so it runs in a subprocess of this test binary.
			t.Run("broken_pubsub_fails", func(t *testing.T) {
				t.Parallel()

				cmd := exec.Command(os.Args[0], "-test.run=^TestBrokenPubSubHelper$", "-test.timeout=2m")
				cmd.Env = append(os.Environ(), brokenPubSubEnv+"="+name)
				output, err := cmd.CombinedOutput()

				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "the suite test passed for a broken Pub/Sub, output:\n%s", output)
				assert.Contains(t, string(output), check.expectedFailure)
			})
		})
	}
}

// TestBrokenPubSubHelper runs a suite test against a broken Pub/Sub; it is started only by TestSuiteChecks.
func TestBrokenPubSubHelper(t *testing.T) {
	check, ok := suiteChecks[os.Getenv(brokenPubSubEnv)]
	if !ok {
		t.Skip("started only by TestSuiteChecks")
	}

	runSuiteTest(t, check.suiteTest, check.features, check.defect)
}

func runSuiteTest(
	t *testing.T,
	suiteTest func(*testing.T, tests.TestContext, tests.PubSubConstructor),
	features tests.Features,
	defect defect,
) {
	suiteTest(
		t,
		tests.TestContext{TestID: tests.NewTestID(), Features: features},
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			pubSub := newOrderedPubSub(defect)
			return pubSub, pubSub
		},
	)
}

type defect int

const (
	noDefect defect = iota
	dropsSubscribeCtxValues
	// Only after catching up: an earlier duplicate takes the place of an expected message,
	// so the suite would fail on the missing message even without the exactly-once check.
	redeliversAfterCatchingUp
	// Neighbours have different payloads, so only the total order check can notice the swap.
	// The caller's slice is swapped too, so the check can't take the expected order from it after Publish.
	swapsNeighbours
)

// orderedPubSub exists because no Pub/Sub in this repository guarantees order (GoChannel declares none).
// Every subscription reads the topic from the start, because the suite tests publish before they subscribe.
// It implements only what the suite tests in suiteChecks need.
type orderedPubSub struct {
	defect defect

	lock          sync.Mutex
	logs          map[string][]*message.Message
	closing       chan struct{}
	closeOnce     sync.Once
	subscriptions sync.WaitGroup
}

func newOrderedPubSub(defect defect) *orderedPubSub {
	return &orderedPubSub{
		defect:  defect,
		logs:    map[string][]*message.Message{},
		closing: make(chan struct{}),
	}
}

func (o *orderedPubSub) Publish(topic string, messages ...*message.Message) error {
	if o.defect == swapsNeighbours {
		for i := 0; i+1 < len(messages); i += 2 {
			messages[i], messages[i+1] = messages[i+1], messages[i]
		}
	}

	o.lock.Lock()
	defer o.lock.Unlock()

	o.logs[topic] = append(o.logs[topic], messages...)
	return nil
}

func (o *orderedPubSub) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	msgCtx := ctx
	if o.defect == dropsSubscribeCtxValues {
		msgCtx = context.Background()
	}

	output := make(chan *message.Message)

	o.subscriptions.Add(1)
	go func() {
		defer o.subscriptions.Done()
		defer close(output)

		offset := 0
		redelivered := false

		for {
			msg := o.messageAt(topic, offset)

			if msg == nil && o.defect == redeliversAfterCatchingUp && offset > 0 && !redelivered {
				redelivered = true
				offset--
				continue
			}

			if msg == nil {
				select {
				case <-time.After(time.Millisecond):
					continue
				case <-ctx.Done():
					return
				case <-o.closing:
					return
				}
			}

			msg.SetContext(msgCtx)

			select {
			case output <- msg:
			case <-ctx.Done():
				return
			case <-o.closing:
				return
			}

			select {
			case <-msg.Acked():
				offset++
			case <-msg.Nacked():
			case <-ctx.Done():
				return
			case <-o.closing:
				return
			}
		}
	}()

	return output, nil
}

// messageAt returns a copy because every delivery needs its own Ack and Nack and its own context,
// while the messages in the log are shared by all subscriptions and redeliveries.
func (o *orderedPubSub) messageAt(topic string, offset int) *message.Message {
	o.lock.Lock()
	defer o.lock.Unlock()

	if offset >= len(o.logs[topic]) {
		return nil
	}

	return o.logs[topic][offset].Copy()
}

func (o *orderedPubSub) Close() error {
	o.closeOnce.Do(func() { close(o.closing) })
	o.subscriptions.Wait()
	return nil
}
