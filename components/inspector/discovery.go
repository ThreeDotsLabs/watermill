package inspector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/topology"
)

const (
	// DefaultDiscoveryTopic is the topic used by PubSubDiscovery when no topic is given.
	DefaultDiscoveryTopic = "watermill_inspector_discovery"

	announceInterval = 3 * time.Second
	// peerTimeout is the time after which a service that stopped announcing itself is forgotten.
	peerTimeout = 12 * time.Second
	// maxAnnouncementAge limits how old (or how far in the future) a signed announcement can be.
	// It has to be bigger than the difference between clocks of services.
	maxAnnouncementAge = 2 * time.Minute
	// listenRetryInterval is the time between attempts to listen to announcements.
	listenRetryInterval = 500 * time.Millisecond
)

// Announcement tells other services where the inspector of a service is.
type Announcement struct {
	// Service is the name of the service. Instances of one service have the same name.
	Service string `json:"service"`
	// Instance identifies the running process.
	Instance string `json:"instance"`
	// URL is the address of the inspector handler that other services can reach.
	URL string `json:"url"`
	// Time is the time of the announcement, as Unix seconds.
	Time int64 `json:"time"`
	// Signature proves that the announcement comes from a service that knows the token.
	Signature string `json:"signature"`
}

func (a Announcement) sign(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = fmt.Fprintf(mac, "%q %q %q %d", a.Service, a.Instance, a.URL, a.Time)
	return hex.EncodeToString(mac.Sum(nil))
}

func (a Announcement) verify(token string, now time.Time) bool {
	age := now.Sub(time.Unix(a.Time, 0))
	if age > maxAnnouncementAge || age < -maxAnnouncementAge {
		return false
	}
	return hmac.Equal([]byte(a.sign(token)), []byte(a.Signature))
}

// Discovery lets inspectors of different services find each other.
//
// It only passes announcements around. The inspector signs and verifies them.
type Discovery interface {
	// Announce sends the announcement to all other services.
	Announce(ctx context.Context, announcement Announcement) error
	// Listen returns announcements of all services, until ctx is done.
	// Every instance of every service has to receive every announcement.
	Listen(ctx context.Context) (<-chan Announcement, error)
}

// PubSubDiscovery is a Discovery that uses a topic of a Pub/Sub that all services are connected to.
type PubSubDiscovery struct {
	publisher  message.Publisher
	subscriber message.Subscriber
	topic      string
}

// NewPubSubDiscovery creates a Discovery that publishes announcements to a topic.
//
// Every instance has to receive all announcements, so the subscriber can't share
// a consumer group (or its equivalent) with other instances.
// With Kafka, use a subscriber with no consumer group.
//
// When the topic is empty, DefaultDiscoveryTopic is used.
func NewPubSubDiscovery(publisher message.Publisher, subscriber message.Subscriber, topic string) *PubSubDiscovery {
	if topic == "" {
		topic = DefaultDiscoveryTopic
	}

	// Announcements are not a part of the application, so they are not shown in the inspector.
	topology.Enable().IgnoreTopic(topic)

	return &PubSubDiscovery{publisher: publisher, subscriber: subscriber, topic: topic}
}

func (d *PubSubDiscovery) Announce(_ context.Context, announcement Announcement) error {
	payload, err := json.Marshal(announcement)
	if err != nil {
		return err
	}

	return d.publisher.Publish(d.topic, message.NewMessage(watermill.NewUUID(), payload))
}

func (d *PubSubDiscovery) Listen(ctx context.Context) (<-chan Announcement, error) {
	messages, err := d.subscriber.Subscribe(ctx, d.topic)
	if err != nil {
		return nil, fmt.Errorf("cannot subscribe to the discovery topic: %w", err)
	}

	announcements := make(chan Announcement)
	go func() {
		defer close(announcements)

		for msg := range messages {
			var announcement Announcement
			err := json.Unmarshal(msg.Payload, &announcement)
			msg.Ack()
			if err != nil {
				continue
			}

			select {
			case announcements <- announcement:
			case <-ctx.Done():
				return
			}
		}
	}()

	return announcements, nil
}

// peer is an inspector of another instance, found with Discovery.
type peer struct {
	Service  string
	Instance string
	URL      string

	lastSeen time.Time
}

// peers announces this instance and keeps the list of other instances.
type peers struct {
	discovery Discovery
	token     string
	self      Announcement
	logger    watermill.LoggerAdapter

	lock  sync.Mutex
	known map[string]peer
}

func startPeers(ctx context.Context, discovery Discovery, token string, self Announcement, logger watermill.LoggerAdapter) (*peers, error) {
	p := &peers{
		discovery: discovery,
		token:     token,
		self:      self,
		logger:    logger,
		known:     map[string]peer{},
	}

	// Announcing before listening: with many Pub/Subs, the first publish creates the topic.
	if err := discovery.Announce(ctx, p.signedSelf()); err != nil {
		return nil, fmt.Errorf("cannot announce the inspector: %w", err)
	}

	// The new topic may not be visible to the subscriber right away.
	announcements, err := p.subscribe(ctx, 10)
	if err != nil {
		return nil, err
	}

	go p.listen(ctx, announcements)
	go p.announce(ctx)

	return p, nil
}

// subscribe calls Discovery.Listen until it succeeds, ctx is done, or the attempts run out.
// With maxAttempts 0, it tries until ctx is done.
func (p *peers) subscribe(ctx context.Context, maxAttempts int) (<-chan Announcement, error) {
	for attempt := 1; ; attempt++ {
		announcements, err := p.discovery.Listen(ctx)
		if err == nil {
			return announcements, nil
		}
		if maxAttempts > 0 && attempt >= maxAttempts {
			return nil, err
		}
		p.logger.Debug("Cannot listen to inspector announcements, retrying", watermill.LogFields{"err": err.Error()})

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(listenRetryInterval):
		}
	}
}

func (p *peers) signedSelf() Announcement {
	announcement := p.self
	announcement.Time = time.Now().Unix()
	announcement.Signature = announcement.sign(p.token)
	return announcement
}

// listen keeps the list of other instances up to date, until ctx is done.
// When the announcements end earlier, for example because the subscriber was closed, it listens again.
func (p *peers) listen(ctx context.Context, announcements <-chan Announcement) {
	for {
		select {
		case <-ctx.Done():
			return

		case announcement, ok := <-announcements:
			if ok {
				p.receive(announcement)
				continue
			}

			p.logger.Info("Inspector announcements have ended, listening again", nil)
			select {
			case <-ctx.Done():
				return
			case <-time.After(listenRetryInterval):
			}

			var err error
			announcements, err = p.subscribe(ctx, 0)
			if err != nil {
				// ctx is done.
				return
			}
		}
	}
}

func (p *peers) receive(announcement Announcement) {
	if announcement.Instance == p.self.Instance {
		return
	}
	if !announcement.verify(p.token, time.Now()) {
		p.logger.Debug("Ignoring an inspector announcement with a wrong signature", watermill.LogFields{
			"service": announcement.Service,
			"url":     announcement.URL,
		})
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.known[announcement.Instance] = peer{
		Service:  announcement.Service,
		Instance: announcement.Instance,
		URL:      announcement.URL,
		lastSeen: time.Now(),
	}
}

func (p *peers) announce(ctx context.Context) {
	ticker := time.NewTicker(announceInterval)
	defer ticker.Stop()

	for {
		if err := p.discovery.Announce(ctx, p.signedSelf()); err != nil {
			p.logger.Error("Cannot announce the inspector", err, nil)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// list returns the instances that have announced themselves recently, by instance.
func (p *peers) list() map[string]peer {
	if p == nil {
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	alive := make(map[string]peer, len(p.known))
	for instance, known := range p.known {
		if time.Since(known.lastSeen) > peerTimeout {
			delete(p.known, instance)
			continue
		}
		alive[instance] = known
	}
	return alive
}
