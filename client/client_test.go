package client

import (
	"context"
	"net/url"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	mqttpublisher "github.com/tsarna/vinculum-mqtt/publisher"
	mqttsubscriber "github.com/tsarna/vinculum-mqtt/subscriber"
	"go.uber.org/zap"
)

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func validConfig() ClientConfig {
	return ClientConfig{
		ServerURLs: []*url.URL{mustURL("mqtt://localhost:1883")},
		ClientID:   "test-client",
	}
}

// captureSubscriber records OnEvent calls.
type captureSubscriber struct {
	bus.BaseSubscriber
}

func (s *captureSubscriber) OnEvent(_ context.Context, _ string, _ any, _ map[string]string) error {
	return nil
}

// ── NewClient ─────────────────────────────────────────────────────────────────

func TestNewClient_EmptyServerURLs(t *testing.T) {
	_, err := NewClient(ClientConfig{})
	assert.Error(t, err)
}

func TestNewClient_Valid(t *testing.T) {
	c, err := NewClient(validConfig())
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestNewClient_NilLoggerUsesNop(t *testing.T) {
	c, err := NewClient(validConfig())
	require.NoError(t, err)
	assert.NotNil(t, c.logger)
}

// ── AddPublisher / AddSubscriber ──────────────────────────────────────────────

func TestAddPublisher_Accumulates(t *testing.T) {
	c, _ := NewClient(validConfig())
	p1, _ := mqttpublisher.NewPublisher().Build()
	p2, _ := mqttpublisher.NewPublisher().Build()
	c.AddPublisher(p1)
	c.AddPublisher(p2)
	assert.Len(t, c.publishers, 2)
}

func TestAddSubscriber_Accumulates(t *testing.T) {
	c, _ := NewClient(validConfig())
	s1, _ := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "a/#"}).
		WithSubscriber(&captureSubscriber{}).
		Build()
	s2, _ := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "b/#"}).
		WithSubscriber(&captureSubscriber{}).
		Build()
	c.AddSubscriber(s1)
	c.AddSubscriber(s2)
	assert.Len(t, c.subscribers, 2)
}

// ── buildSubscribeOptions ─────────────────────────────────────────────────────

func TestBuildSubscribeOptions_NoSubscribers(t *testing.T) {
	c, _ := NewClient(validConfig())
	assert.Empty(t, c.buildSubscribeOptions())
}

func TestBuildSubscribeOptions_SingleSubscriberNoSharedGroup(t *testing.T) {
	c, _ := NewClient(validConfig())
	s, _ := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensor/+deviceId/data", QoS: 1}).
		WithSubscriber(&captureSubscriber{}).
		Build()
	c.AddSubscriber(s)

	opts := c.buildSubscribeOptions()
	require.Len(t, opts, 1)
	assert.Equal(t, "sensor/+/data", opts[0].Topic)
	assert.Equal(t, byte(1), opts[0].QoS)
}

func TestBuildSubscribeOptions_SharedGroupPrefix(t *testing.T) {
	c, _ := NewClient(validConfig())
	s, _ := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensor/#", QoS: 0}).
		WithSubscriber(&captureSubscriber{}).
		WithSharedGroup("workers").
		Build()
	c.AddSubscriber(s)

	opts := c.buildSubscribeOptions()
	require.Len(t, opts, 1)
	assert.Equal(t, "$share/workers/sensor/#", opts[0].Topic)
}

func TestBuildSubscribeOptions_MultipleSubscribers(t *testing.T) {
	c, _ := NewClient(validConfig())
	for _, pattern := range []string{"a/#", "b/#", "c/#"} {
		s, _ := mqttsubscriber.NewSubscriber().
			WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: pattern}).
			WithSubscriber(&captureSubscriber{}).
			Build()
		c.AddSubscriber(s)
	}
	assert.Len(t, c.buildSubscribeOptions(), 3)
}

// ── stripSharePrefix ──────────────────────────────────────────────────────────

func TestStripSharePrefix_NoPrefix(t *testing.T) {
	assert.Equal(t, "sensor/+/data", stripSharePrefix("sensor/+/data"))
}

func TestStripSharePrefix_WithPrefix(t *testing.T) {
	assert.Equal(t, "sensor/+/data", stripSharePrefix("$share/workers/sensor/+/data"))
}

func TestStripSharePrefix_HashWildcard(t *testing.T) {
	assert.Equal(t, "sensor/#", stripSharePrefix("$share/grp/sensor/#"))
}

// ── buildWillMessage ──────────────────────────────────────────────────────────

func TestBuildWillMessage_Nil(t *testing.T) {
	assert.Nil(t, buildWillMessage(nil))
}

func TestBuildWillMessage_Full(t *testing.T) {
	cfg := &WillConfig{Topic: "t", Payload: []byte("bye"), QoS: 1, Retain: true}
	msg := buildWillMessage(cfg)
	require.NotNil(t, msg)
	assert.Equal(t, "t", msg.Topic)
	assert.Equal(t, []byte("bye"), msg.Payload)
	assert.Equal(t, byte(1), msg.QoS)
	assert.True(t, msg.Retain)
}

// ── Stop before Start ─────────────────────────────────────────────────────────

func TestStop_BeforeStart_IsNoop(t *testing.T) {
	c, _ := NewClient(validConfig())
	err := c.Stop(context.Background())
	assert.NoError(t, err)
}

// ── buildRouter ───────────────────────────────────────────────────────────────

func TestBuildRouter_RegistersHandlersForEachBrokerTopic(t *testing.T) {
	c, _ := NewClient(validConfig())
	s, _ := mqttsubscriber.NewSubscriber().
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "sensor/+deviceId/data"}).
		WithSubscription(mqttsubscriber.TopicSubscription{MQTTPattern: "status/#"}).
		WithSubscriber(&captureSubscriber{}).
		Build()
	c.AddSubscriber(s)

	// buildRouter should not panic and should register handlers.
	// We can't easily introspect the router, but we verify it returns without error.
	router := c.buildRouter(context.Background())
	assert.NotNil(t, router)
}

// ── keepAlive conversion ──────────────────────────────────────────────────────

func TestBuildAutopahoConfig_KeepAlive(t *testing.T) {
	cfg := validConfig()
	cfg.KeepAlive = 30e9 // 30s in nanoseconds
	c, _ := NewClient(cfg)
	startReady := make(chan struct{})
	firstConn := true
	acfg := c.buildAutopahoConfig(context.Background(), paho.NewStandardRouter(), startReady, &firstConn, nil)
	assert.Equal(t, uint16(30), acfg.KeepAlive)
}

// ── reconnect limiter ─────────────────────────────────────────────────────────

func newTestLimiter(max int) (*reconnectLimiter, *bool) {
	stopped := false
	return &reconnectLimiter{
		max:    max,
		logger: zap.NewNop(),
		stop:   func() { stopped = true },
	}, &stopped
}

// The default and the behaviour before the field existed: reconnect forever.
// Zero is the Go zero value, so this is the case that must not change.
func TestReconnectLimiterUnlimitedByDefault(t *testing.T) {
	for _, max := range []int{0, -1} {
		l, stopped := newTestLimiter(max)
		l.connected()
		for i := 0; i < 1000; i++ {
			l.failed()
		}
		assert.False(t, *stopped, "max=%d should reconnect forever", max)
	}
}

func TestReconnectLimiterGivesUpAtTheLimit(t *testing.T) {
	l, stopped := newTestLimiter(3)
	l.connected()

	l.failed()
	l.failed()
	assert.False(t, *stopped, "not yet at the limit")

	l.failed()
	assert.True(t, *stopped, "third consecutive failure reaches max=3")
}

// The limit bounds one outage, not the client's lifetime, so a success in
// between wipes the slate. Without this a long-lived client would accumulate
// unrelated failures and eventually give up during a perfectly ordinary blip.
func TestReconnectLimiterResetsOnSuccess(t *testing.T) {
	l, stopped := newTestLimiter(3)
	l.connected()

	l.failed()
	l.failed()
	l.connected() // outage over
	l.failed()
	l.failed()
	assert.False(t, *stopped, "two failures either side of a success is not three in a row")

	l.failed()
	assert.True(t, *stopped)
}

// MaxReconnectAttempts governs reconnection. A broker that is not listening yet
// at process start is ordinary, and giving up there would turn a slow
// dependency into a startup failure.
func TestReconnectLimiterDoesNotBoundTheInitialConnection(t *testing.T) {
	l, stopped := newTestLimiter(2)

	for i := 0; i < 50; i++ {
		l.failed()
	}
	assert.False(t, *stopped, "never connected, so nothing to reconnect")

	// Once up, the limit applies from a clean count.
	l.connected()
	l.failed()
	assert.False(t, *stopped)
	l.failed()
	assert.True(t, *stopped)
}

// A nil limiter is the "no limit configured" path used by buildAutopahoConfig's
// callers in tests; it must be inert rather than panic.
func TestReconnectLimiterNilIsInert(t *testing.T) {
	var l *reconnectLimiter
	assert.NotPanics(t, func() {
		l.connected()
		l.failed()
	})
}
