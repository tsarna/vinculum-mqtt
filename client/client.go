package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	mqttpublisher "github.com/tsarna/vinculum-mqtt/publisher"
	mqttsubscriber "github.com/tsarna/vinculum-mqtt/subscriber"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// MQTTClient manages a single autopaho.ConnectionManager, wiring zero or more
// MQTTPublishers and MQTTSubscribers to a shared MQTT connection.
//
// Call AddPublisher / AddSubscriber before Start. Start blocks until the first
// connection is established and all subscriptions are registered. Stop sends a
// graceful DISCONNECT and waits for the connection to close.
type MQTTClient struct {
	cfg         ClientConfig
	publishers  []*mqttpublisher.MQTTPublisher
	subscribers []*mqttsubscriber.MQTTSubscriber
	metrics     *ClientMetrics
	logger      *zap.Logger

	mu sync.Mutex
	cm *autopaho.ConnectionManager
	// stopRun cancels the context handed to autopaho, which is the only way to
	// break out of its reconnect loop from the outside. See reconnectLimiter.
	stopRun context.CancelFunc
	// connected mirrors the connection gauge: true between OnConnectionUp and
	// the OnConnectionDown that follows it. Read by IsConnected.
	connected bool
}

// reconnectLimiter enforces ClientConfig.MaxReconnectAttempts.
//
// autopaho's reconnect loop cannot be stopped by returning anything from a
// callback: establishServerConnection retries until its context is cancelled,
// and the one callback with a bool return, OnConnectionDown, is consulted once
// per dropped connection rather than once per failed attempt. So the limit is
// enforced by counting failures in OnConnectError and cancelling the context
// the connection manager was built with.
//
// The count is of *consecutive* failures and resets on every success, so the
// limit bounds one outage rather than the client's lifetime. Counting does not
// begin until the first connection has come up: the field governs reconnection,
// and a broker that is not listening yet at process start is an ordinary
// situation rather than one to give up on.
type reconnectLimiter struct {
	max    int
	logger *zap.Logger
	stop   context.CancelFunc

	mu            sync.Mutex
	connectedOnce bool
	failures      int
}

// connected records a successful connection, which arms the limiter and clears
// any failures counted during the outage that just ended.
func (l *reconnectLimiter) connected() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.connectedOnce = true
	l.failures = 0
}

// failed records one failed connection attempt and gives up if that reaches the
// limit. Called from OnConnectError, which autopaho invokes per attempt.
func (l *reconnectLimiter) failed() {
	if l == nil || l.max <= 0 {
		return
	}

	l.mu.Lock()
	if !l.connectedOnce {
		l.mu.Unlock()
		return // still working on the initial connection, which is unbounded
	}
	l.failures++
	reached := l.failures >= l.max
	failures := l.failures
	l.mu.Unlock()

	if !reached {
		return
	}
	l.logger.Error("mqtt client: giving up reconnection attempts",
		zap.Int("attempts", failures),
		zap.Int("max_reconnect_attempts", l.max))
	l.stop()
}

// NewClient constructs an MQTTClient. Validates that at least one server URL
// is provided.
func NewClient(cfg ClientConfig) (*MQTTClient, error) {
	if len(cfg.ServerURLs) == 0 {
		return nil, errors.New("mqtt client: at least one server URL is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	var meter metric.Meter
	if cfg.MeterProvider != nil {
		meter = cfg.MeterProvider.Meter("github.com/tsarna/vinculum-mqtt/client")
	}
	return &MQTTClient{
		cfg:     cfg,
		metrics: NewClientMetrics(cfg.ClientName, meter),
		logger:  logger,
	}, nil
}

// AddPublisher registers a publisher. Must be called before Start.
// At Start time the publisher receives the shared publish function.
func (c *MQTTClient) AddPublisher(p *mqttpublisher.MQTTPublisher) {
	c.publishers = append(c.publishers, p)
}

// AddSubscriber registers a subscriber. Must be called before Start.
// At Start time the subscriber's broker topics are subscribed to and its
// HandleMessage is wired into the MQTT message router.
func (c *MQTTClient) AddSubscriber(s *mqttsubscriber.MQTTSubscriber) {
	c.subscribers = append(c.subscribers, s)
}

// IsConnected reports whether the client currently holds a live connection to
// the broker: OnConnectionUp has fired without a subsequent OnConnectionDown.
//
// It is a snapshot, not a guarantee — the connection may drop between this call
// and the next publish — which is what makes it useful for a health probe and
// useless as a precondition. A caller that wants to publish should publish and
// handle the error.
//
// False both before Start and during an outage autopaho's reconnect loop has
// not yet repaired, so a host reporting readiness from this correctly says "not
// ready" while the broker is away and recovers on its own.
func (c *MQTTClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// setConnected records the connection state IsConnected reports.
func (c *MQTTClient) setConnected(connected bool) {
	c.mu.Lock()
	c.connected = connected
	c.mu.Unlock()
}

// Start connects to the MQTT broker, registers all subscriptions, injects the
// publish function into all publishers, and returns. Blocks until the first
// connection is fully established (OnConnectionUp has run) or ctx is cancelled.
func (c *MQTTClient) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.cm != nil {
		c.mu.Unlock()
		return errors.New("mqtt client: already started")
	}
	c.mu.Unlock()

	// startReady is closed by OnConnectionUp after the first successful
	// connection, subscriptions, and publish-func injection.
	startReady := make(chan struct{})
	firstConn := true

	// runCtx is cancellable independently of the caller's ctx so the reconnect
	// limiter can shut the connection manager down without the caller having to
	// give up its own context. Cancelling the caller's ctx still works: runCtx
	// is derived from it.
	runCtx, stopRun := context.WithCancel(ctx)
	limiter := &reconnectLimiter{
		max:    c.cfg.MaxReconnectAttempts,
		logger: c.logger,
		stop:   stopRun,
	}

	router := c.buildRouter(runCtx)
	acfg := c.buildAutopahoConfig(runCtx, router, startReady, &firstConn, limiter)

	cm, err := autopaho.NewConnection(runCtx, acfg)
	if err != nil {
		stopRun()
		return fmt.Errorf("mqtt client: create connection manager: %w", err)
	}

	// Wait until OnConnectionUp has completed for the first connection.
	select {
	case <-startReady:
		// First connection up, subscriptions registered, publishers wired.
	case <-ctx.Done():
		stopRun()
		return fmt.Errorf("mqtt client: context cancelled waiting for first connection: %w", ctx.Err())
	case <-cm.Done():
		stopRun()
		return fmt.Errorf("mqtt client: connection manager terminated before first connection")
	}

	c.mu.Lock()
	c.cm = cm
	c.stopRun = stopRun
	c.mu.Unlock()

	return nil
}

// Stop sends a graceful DISCONNECT packet and waits for the connection manager
// to shut down. Safe to call before Start (returns nil).
func (c *MQTTClient) Stop(ctx context.Context) error {
	c.mu.Lock()
	cm := c.cm
	stopRun := c.stopRun
	c.mu.Unlock()

	// Release runCtx once the graceful disconnect below has finished with it.
	// Doing it first would cancel the connection manager out from under the
	// DISCONNECT packet, turning a clean shutdown into a dropped connection.
	if stopRun != nil {
		defer stopRun()
	}

	if cm == nil {
		return nil
	}

	// Cleared here as well as in OnConnectionDown: a graceful DISCONNECT is a
	// deliberate teardown rather than a dropped connection, so the callback is
	// not guaranteed to run for it, and a stopped client must not keep
	// reporting itself connected.
	defer c.setConnected(false)

	return cm.Disconnect(ctx)
}

// buildRouter creates a paho.StandardRouter that routes each incoming message
// to the HandleMessage of the appropriate MQTTSubscriber.
func (c *MQTTClient) buildRouter(ctx context.Context) *paho.StandardRouter {
	router := paho.NewStandardRouter()
	for _, sub := range c.subscribers {
		sub := sub // capture loop variable
		for _, opt := range sub.BrokerSubscriptions() {
			topic := opt.Topic
			// Strip shared subscription prefix for router registration —
			// the router matches on the concrete topic, not the $share prefix.
			router.RegisterHandler(stripSharePrefix(topic), func(pub *paho.Publish) {
				if err := sub.HandleMessage(ctx, pub); err != nil {
					c.logger.Error("mqtt subscriber: handle message error",
						zap.String("topic", pub.Topic),
						zap.Error(err))
				}
			})
		}
	}
	return router
}

// buildSubscribeOptions collects all broker subscription options from all subscribers.
func (c *MQTTClient) buildSubscribeOptions() []paho.SubscribeOptions {
	var opts []paho.SubscribeOptions
	for _, sub := range c.subscribers {
		opts = append(opts, sub.BrokerSubscriptions()...)
	}
	return opts
}

// buildAutopahoConfig constructs the autopaho.ClientConfig. startReady is
// closed from within OnConnectionUp on the first connection after publishers
// are wired. firstConn is a pointer to a local bool in Start().
func (c *MQTTClient) buildAutopahoConfig(
	ctx context.Context,
	router *paho.StandardRouter,
	startReady chan struct{},
	firstConn *bool,
	limiter *reconnectLimiter,
) autopaho.ClientConfig {
	keepAliveSecs := uint16(0)
	if c.cfg.KeepAlive > 0 {
		keepAliveSecs = uint16(c.cfg.KeepAlive.Seconds())
	}

	acfg := autopaho.ClientConfig{
		ServerUrls:                    c.cfg.ServerURLs,
		TlsCfg:                        c.cfg.TLSConfig,
		KeepAlive:                     keepAliveSecs,
		CleanStartOnInitialConnection: c.cfg.CleanStart,
		SessionExpiryInterval:         c.cfg.SessionExpiryInterval,
		ConnectUsername:               c.cfg.Username,
		ConnectPassword:               c.cfg.Password,
		ReconnectBackoff:              c.cfg.ReconnectBackoffFunc,
		WillMessage:                   buildWillMessage(c.cfg.WillMessage),

		OnConnectError: func(err error) {
			c.logger.Warn("mqtt client: connection attempt failed", zap.Error(err))
			limiter.failed()
		},

		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			limiter.connected()

			// Subscribe to all broker topics on every (re)connection.
			opts := c.buildSubscribeOptions()
			if len(opts) > 0 {
				if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: opts}); err != nil {
					c.logger.Error("mqtt client: subscribe failed", zap.Error(err))
				}
			}

			c.setConnected(true)
			c.metrics.SetConnected(ctx, true)

			if !*firstConn {
				c.metrics.IncrReconnects(ctx)
			}

			// Call the on_connect lifecycle hook.
			if c.cfg.OnConnect != nil {
				c.cfg.OnConnect(ctx)
			}

			// On the very first connection: wire publishers and signal Start().
			if *firstConn {
				*firstConn = false
				for _, p := range c.publishers {
					p.SetPublishFunc(cm.Publish)
				}
				close(startReady)
			}
		},

		OnConnectionDown: func() bool {
			c.setConnected(false)
			c.metrics.SetConnected(ctx, false)
			if c.cfg.OnDisconnect != nil {
				c.cfg.OnDisconnect(ctx)
			}
			return true // always reconnect
		},

		AttemptConnection: makeAttemptConnection(c.cfg.TLSConfig),
		ClientConfig: paho.ClientConfig{
			Router:   router,
			ClientID: c.cfg.ClientID,
		},
	}

	return acfg
}

// buildWillMessage converts a WillConfig to a paho.WillMessage, or nil.
func buildWillMessage(cfg *WillConfig) *paho.WillMessage {
	if cfg == nil {
		return nil
	}
	return &paho.WillMessage{
		Topic:   cfg.Topic,
		Payload: cfg.Payload,
		QoS:     cfg.QoS,
		Retain:  cfg.Retain,
	}
}

// stripSharePrefix removes the "$share/<group>/" prefix from a topic so it can
// be registered with the paho router, which matches on concrete topics.
// "sensor/+/data" and "$share/workers/sensor/+/data" both register as "sensor/+/data".
func stripSharePrefix(topic string) string {
	if len(topic) > 7 && topic[:7] == "$share/" {
		// Find the second slash: "$share/groupname/actual/topic"
		rest := topic[7:]
		for i, ch := range rest {
			if ch == '/' {
				return rest[i+1:]
			}
		}
	}
	return topic
}
