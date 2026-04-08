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
		metrics: NewClientMetrics(meter),
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

	router := c.buildRouter(ctx)
	acfg := c.buildAutopahoConfig(ctx, router, startReady, &firstConn)

	cm, err := autopaho.NewConnection(ctx, acfg)
	if err != nil {
		return fmt.Errorf("mqtt client: create connection manager: %w", err)
	}

	// Wait until OnConnectionUp has completed for the first connection.
	select {
	case <-startReady:
		// First connection up, subscriptions registered, publishers wired.
	case <-ctx.Done():
		return fmt.Errorf("mqtt client: context cancelled waiting for first connection: %w", ctx.Err())
	case <-cm.Done():
		return fmt.Errorf("mqtt client: connection manager terminated before first connection")
	}

	c.mu.Lock()
	c.cm = cm
	c.mu.Unlock()

	return nil
}

// Stop sends a graceful DISCONNECT packet and waits for the connection manager
// to shut down. Safe to call before Start (returns nil).
func (c *MQTTClient) Stop(ctx context.Context) error {
	c.mu.Lock()
	cm := c.cm
	c.mu.Unlock()

	if cm == nil {
		return nil
	}

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

		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			// Subscribe to all broker topics on every (re)connection.
			opts := c.buildSubscribeOptions()
			if len(opts) > 0 {
				if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: opts}); err != nil {
					c.logger.Error("mqtt client: subscribe failed", zap.Error(err))
				}
			}

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
