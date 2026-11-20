package tests

// Temper claims for the universal suite. Each test states the invariant it
// proves for the Pub/Sub under test, and the pubsub.features lattice member it
// proves when the Pub/Sub's Features claim that member. Temper records these
// claims only under `temper run`; otherwise temper.Covers does nothing.
//
// The subject is the Pub/Sub under test. TEMPER_SUBJECT names it. When one
// test binary runs several Pub/Subs, TEMPER_SUBJECT_MAP maps a prefix of the
// top-level test name to a subject: "TestMySQL=pubsub.sql.mysql,TestPostgreSQL=pubsub.sql.postgres".

import (
	"os"
	"strings"
	"testing"

	"github.com/turbolytics/temper"
)

var invariantsByTest = map[string][]string{
	"TestPublishSubscribe":                  {"delivery.at_least_once"},
	"TestConcurrentSubscribe":               {"delivery.concurrent_subscribers"},
	"TestConcurrentSubscribeMultipleTopics": {"delivery.concurrent_subscribers"},
	"TestResendOnError":                     {"ack.nack_redelivers"},
	"TestNoAck":                             {"ack.next_only_after_ack"},
	"TestContinueAfterSubscribeClose":       {"shutdown.subscriber_close_keeps_offsets"},
	"TestConcurrentClose":                   {"shutdown.concurrent_close_safe"},
	"TestContinueAfterErrors":               {"handling.continues_after_errors"},
	"TestPublisherClose":                    {"shutdown.publisher_close_flushes"},
	"TestTopic":                             {"delivery.topics_isolated"},
	"TestMessageCtx":                        {"ack.ack_cancels_message_context"},
	"TestSubscribeCtx":                      {"shutdown.subscribe_ctx_cancel_closes"},
	"TestReconnect":                         {"delivery.no_loss_across_restart"},
}

// featureClaims returns the pubsub.features members a test proves, given what
// the Pub/Sub claims.
func featureClaims(test string, f Features) []string {
	var out []string
	add := func(claimed bool, member string) {
		if claimed {
			out = append(out, "pubsub.features/"+member)
		}
	}
	switch test {
	case "TestConsumerGroups":
		add(f.ConsumerGroups, "consumer_groups")
	case "TestPublishSubscribe", "TestReconnect":
		add(f.ExactlyOnceDelivery, "exactly_once")
	case "TestPublishSubscribeInOrder":
		add(f.GuaranteedOrder, "guaranteed_order")
		add(f.GuaranteedOrderWithSingleSubscriber, "guaranteed_order_single_subscriber")
	case "TestContinueAfterErrors":
		add(f.Persistent, "persistent")
	case "TestNewSubscriberReceivesOldMessages":
		add(f.NewSubscriberReceivesOldMessages, "new_subscriber_receives_old_messages")
	case "TestMessageCtx":
		add(f.ContextPreserved, "context_preserved")
	}
	return out
}

func temperSubject(t *testing.T) string {
	root := strings.SplitN(t.Name(), "/", 2)[0]
	for _, entry := range strings.Split(os.Getenv("TEMPER_SUBJECT_MAP"), ",") {
		prefix, subject, ok := strings.Cut(entry, "=")
		if ok && strings.HasPrefix(root, prefix) {
			return subject
		}
	}
	return os.Getenv("TEMPER_SUBJECT")
}

// claimTemper states what one suite test proves. It runs first in the test,
// before anything registers a cleanup.
func claimTemper(t *testing.T, test string, f Features) {
	subject := temperSubject(t)
	if subject == "" {
		return
	}
	if test == "TestPublishSubscribe" {
		temper.Covers(t, subject) // the Pub/Sub's own feature
	}
	for _, inv := range invariantsByTest[test] {
		temper.Covers(t, inv, temper.Subject(subject))
	}
	for _, member := range featureClaims(test, f) {
		temper.Covers(t, member, temper.Subject(subject))
	}
}
