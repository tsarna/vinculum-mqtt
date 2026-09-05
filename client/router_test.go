package client

import (
	"context"
	"sync"
	"testing"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	mqttsubscriber "github.com/tsarna/vinculum-mqtt/subscriber"
)

// countingSubscriber records every delivery, so a message arriving twice is
// visible rather than merely plausible.
type countingSubscriber struct {
	bus.BaseSubscriber
	mu     sync.Mutex
	topics []string
}

func (s *countingSubscriber) OnEvent(_ context.Context, topic string, _ any, _ map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = append(s.topics, topic)
	return nil
}

func (s *countingSubscriber) delivered() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.topics...)
}

// publishOn is the packet a broker would hand paho for one inbound message.
func publishOn(topic string) *packets.Publish {
	return &packets.Publish{
		Topic:      topic,
		Payload:    []byte(`"hello"`),
		Properties: &packets.Properties{},
	}
}

// routeTo builds the client's real router over one subscriber with the given
// patterns and routes one message through it, returning what reached the bus.
func routeTo(t *testing.T, topic string, patterns ...string) []string {
	t.Helper()

	c, err := NewClient(validConfig())
	require.NoError(t, err)

	dest := &countingSubscriber{}
	sb := mqttsubscriber.NewSubscriber()
	for _, p := range patterns {
		sb = sb.WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: p})
	}
	s, err := sb.WithSubscriber(dest).Build()
	require.NoError(t, err)
	c.AddSubscriber(s)

	c.buildRouter(context.Background()).Route(publishOn(topic))
	return dest.delivered()
}

// One inbound PUBLISH is one delivery per subscriber, however many of that
// subscriber's own subscriptions happen to match it.
//
// Overlap is not a user error: a catch-all beside a more specific filter with
// its own vinculum_topic is a reasonable thing to write. The subscriber then
// picks which subscription applies, which is what decides the derived topic and
// the extracted fields — and it already picks exactly one.
func TestOverlappingSubscriptionsDeliverOnce(t *testing.T) {
	got := routeTo(t, "sensors/a/temp", "sensors/#", "sensors/+/temp")
	assert.Equal(t, []string{"sensors/a/temp"}, got,
		"two matching subscriptions on one subscriber is still one message")
}

// Three-way overlap, so a fix that merely deduplicates adjacent pairs does not
// pass by accident.
func TestThreeOverlappingSubscriptionsDeliverOnce(t *testing.T) {
	got := routeTo(t, "sensors/a/temp", "#", "sensors/#", "sensors/+/temp")
	assert.Len(t, got, 1, "one message is one delivery regardless of how deep the overlap goes")
}

// The same defect in its most literal form: two identical patterns on one
// subscriber used to register two routes and produce two deliveries.
func TestDuplicateIdenticalPatternsDeliverOnce(t *testing.T) {
	got := routeTo(t, "sensors/a/temp", "sensors/#", "sensors/#")
	assert.Equal(t, []string{"sensors/a/temp"}, got)
}

// The ordinary case has to keep working: one matching subscription, one
// delivery.
func TestSingleMatchingSubscriptionDeliversOnce(t *testing.T) {
	got := routeTo(t, "sensors/a/temp", "sensors/#")
	assert.Equal(t, []string{"sensors/a/temp"}, got)
}

// A subscriber none of whose subscriptions match is not called at all. Without
// this, "deliver once" could be satisfied by delivering to everyone once.
func TestNonMatchingSubscriberIsNotCalled(t *testing.T) {
	got := routeTo(t, "other/thing", "sensors/#", "sensors/+/temp")
	assert.Empty(t, got, "a subscriber that matches nothing should not be routed to")
}

// Two subscribers both matching is declared fan-out, not the defect: each is a
// separate `receiver` block and each should still be delivered to once.
func TestTwoSubscribersEachGetTheMessageOnce(t *testing.T) {
	c, err := NewClient(validConfig())
	require.NoError(t, err)

	first, second := &countingSubscriber{}, &countingSubscriber{}
	for _, dest := range []*countingSubscriber{first, second} {
		s, err := mqttsubscriber.NewSubscriber().
			WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensors/#"}).
			WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensors/+/temp"}).
			WithSubscriber(dest).
			Build()
		require.NoError(t, err)
		c.AddSubscriber(s)
	}

	c.buildRouter(context.Background()).Route(publishOn("sensors/a/temp"))

	assert.Len(t, first.delivered(), 1, "each receiver gets it once")
	assert.Len(t, second.delivered(), 1, "each receiver gets it once")
}

// A shared subscription reaches the broker as "$share/<group>/<topic>", but the
// broker delivers the concrete topic. Routing on the subscriber's own patterns
// means the prefix never enters the matcher at all.
func TestSharedSubscriptionRoutesOnTheConcreteTopic(t *testing.T) {
	c, err := NewClient(validConfig())
	require.NoError(t, err)

	dest := &countingSubscriber{}
	s, err := mqttsubscriber.NewSubscriber().
		WithSharedGroup("workers").
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensors/#"}).
		WithSubscriber(dest).
		Build()
	require.NoError(t, err)
	c.AddSubscriber(s)

	c.buildRouter(context.Background()).Route(publishOn("sensors/a/temp"))
	assert.Equal(t, []string{"sensors/a/temp"}, dest.delivered())
}

// MQTT 5 lets a broker send a topic alias in place of the topic on every packet
// after the first. Resolving it is the router's job, and a router that did not
// would silently stop delivering mid-stream on any broker that uses aliasing.
func TestTopicAliasIsResolved(t *testing.T) {
	c, err := NewClient(validConfig())
	require.NoError(t, err)

	dest := &countingSubscriber{}
	s, err := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensors/#"}).
		WithSubscriber(dest).
		Build()
	require.NoError(t, err)
	c.AddSubscriber(s)

	router := c.buildRouter(context.Background())

	alias := uint16(7)
	first := publishOn("sensors/a/temp")
	first.Properties.TopicAlias = &alias
	router.Route(first)

	// The broker now sends the alias alone.
	second := &packets.Publish{
		Payload:    []byte(`"hello"`),
		Properties: &packets.Properties{TopicAlias: &alias},
	}
	router.Route(second)

	assert.Equal(t, []string{"sensors/a/temp", "sensors/a/temp"}, dest.delivered(),
		"an aliased publish should route to the topic the alias was registered for")
}

// A leading wildcard does not match a $-prefixed topic. This is the whole
// reason the router matches with topicmatch rather than paho's own matcher,
// which has no such rule: with paho's, a $SYS message could be routed to a
// subscriber whose only pattern is "#" and then rejected by findSubscription as
// unmatched — an error log for a message the router was sure about. Routing and
// dispatch have to agree, and this is where they would not.
func TestLeadingWildcardDoesNotMatchDollarTopics(t *testing.T) {
	got := routeTo(t, "$SYS/broker/uptime", "#")
	assert.Empty(t, got, "# must not reach a $-prefixed topic")
}

// And the subscription that names it explicitly still does, so the rule above
// is the MQTT convention rather than a blanket refusal.
func TestExplicitDollarPatternStillMatches(t *testing.T) {
	got := routeTo(t, "$SYS/broker/uptime", "$SYS/#")
	assert.Equal(t, []string{"$SYS/broker/uptime"}, got)
}

// An alias nobody registered resolves to nothing, and a message with no topic
// cannot be matched against anything. Dropping it with a debug line is all
// there is to do; routing it to every subscriber would be worse.
func TestUnknownAliasIsDropped(t *testing.T) {
	c, err := NewClient(validConfig())
	require.NoError(t, err)

	dest := &countingSubscriber{}
	s, err := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "#"}).
		WithSubscriber(dest).
		Build()
	require.NoError(t, err)
	c.AddSubscriber(s)

	unknown := uint16(99)
	c.buildRouter(context.Background()).Route(&packets.Publish{
		Payload:    []byte(`"hello"`),
		Properties: &packets.Properties{TopicAlias: &unknown},
	})

	assert.Empty(t, dest.delivered(), "an unresolvable alias has no topic to route on")
}

// Properties is a pointer, and paho.PublishFromPacketPublish dereferences it
// unconditionally — StandardRouter panics on a packet without one. Nothing off
// the wire arrives that way, since MQTT 5 always carries a properties field, so
// this pins robustness rather than a fixed bug.
func TestNilPropertiesDoesNotPanic(t *testing.T) {
	c, err := NewClient(validConfig())
	require.NoError(t, err)

	dest := &countingSubscriber{}
	s, err := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensors/#"}).
		WithSubscriber(dest).
		Build()
	require.NoError(t, err)
	c.AddSubscriber(s)

	require.NotPanics(t, func() {
		c.buildRouter(context.Background()).Route(&packets.Publish{
			Topic:   "sensors/a/temp",
			Payload: []byte(`"hello"`),
		})
	})
	assert.Equal(t, []string{"sensors/a/temp"}, dest.delivered())
}

// Aliases belong to the connection that assigned them, and this router outlives
// its connections — it is built once in Start and reused across every
// reconnect. A mapping from a dead session must not answer for a number the new
// one has not assigned.
func TestReconnectForgetsAliases(t *testing.T) {
	r := newSubscriberRouter()

	var seen []string
	r.AddRecipient([]string{"#"}, func(p *paho.Publish) { seen = append(seen, p.Topic) })

	alias := uint16(7)
	first := publishOn("sensors/a/temp")
	first.Properties.TopicAlias = &alias
	r.Route(first)

	r.resetAliases()

	r.Route(&packets.Publish{
		Payload:    []byte(`"hello"`),
		Properties: &packets.Properties{TopicAlias: &alias},
	})

	assert.Equal(t, []string{"sensors/a/temp"}, seen,
		"an alias from the previous connection should no longer resolve")
}

// The method above only matters because the connection callbacks call it, and
// the callbacks are where it would be dropped unnoticed. Both are exercised
// here against the config the client actually builds.
//
// A client with no subscribers and no publishers never touches the
// ConnectionManager in either callback, which is what lets them be invoked
// directly with a nil one.
func TestConnectionCallbacksResetAliases(t *testing.T) {
	for _, tc := range []struct {
		name string
		fire func(acfg autopaho.ClientConfig)
	}{
		{"up", func(acfg autopaho.ClientConfig) { acfg.OnConnectionUp(nil, nil) }},
		{"down", func(acfg autopaho.ClientConfig) { acfg.OnConnectionDown() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(validConfig())
			require.NoError(t, err)

			router := c.buildRouter(context.Background())
			firstConn := false // so OnConnectionUp does not close startReady
			acfg := c.buildAutopahoConfig(
				context.Background(), router, make(chan struct{}), &firstConn, nil)

			alias := uint16(7)
			first := publishOn("sensors/a/temp")
			first.Properties.TopicAlias = &alias
			router.Route(first)
			require.Len(t, router.aliases, 1, "the alias should be registered before the callback")

			tc.fire(acfg)
			assert.Empty(t, router.aliases,
				"OnConnection%s should forget the previous session's aliases", tc.name)
		})
	}
}

// Route snapshots the entry list and dispatches with the lock released, so a
// handler cannot deadlock the router — and a mutator must not write into an
// array a snapshot is still reading. Nothing calls Unregister today, but the
// type is a public paho.Router.
func TestConcurrentRouteAndRegistration(t *testing.T) {
	r := newSubscriberRouter()
	for _, p := range []string{"a/#", "b/#", "c/#"} {
		r.RegisterHandler(p, func(*paho.Publish) {})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				r.Route(publishOn("a/thing"))
			}
		}
	}()

	for range 200 {
		r.UnregisterHandler("b/#")
		r.RegisterHandler("b/#", func(*paho.Publish) {})
	}
	close(stop)
	wg.Wait()
}

// RegisterHandler is part of paho.Router. It is not how this router is
// populated, but it has to behave when something reaches for it.
func TestRegisterAndUnregisterHandler(t *testing.T) {
	r := newSubscriberRouter()

	var calls int
	r.RegisterHandler("sensors/#", func(*paho.Publish) { calls++ })

	r.Route(publishOn("sensors/a/temp"))
	assert.Equal(t, 1, calls)

	r.Route(publishOn("other/thing"))
	assert.Equal(t, 1, calls, "a non-matching topic should not reach the handler")

	r.UnregisterHandler("sensors/#")
	r.Route(publishOn("sensors/a/temp"))
	assert.Equal(t, 1, calls, "an unregistered handler should not be called")
}
