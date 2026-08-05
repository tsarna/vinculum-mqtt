package client

import (
	"context"
	"crypto/tls"
	"net/url"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// ClientConfig holds all configuration for a single MQTT connection.
// It is populated by the vinculum config layer and passed to NewClient.
type ClientConfig struct {
	// ServerURLs — one or more broker URLs (mqtt://, mqtts://, ws://, wss://).
	ServerURLs []*url.URL

	// ClientName — vinculum client name, used in metric attributes.
	ClientName string

	// ClientID — MQTT client identifier. Must be unique per broker connection.
	ClientID string

	// KeepAlive — PINGREQ interval. Converted to seconds for the broker.
	// Zero uses the autopaho default (no keepalive).
	KeepAlive time.Duration

	// CleanStart — whether to request a clean session on the initial connection.
	CleanStart bool

	// SessionExpiryInterval — seconds the broker retains session state after
	// disconnect. Zero means the session ends when the connection closes.
	SessionExpiryInterval uint32

	// TLSConfig — optional TLS configuration for mqtts:// or wss:// connections.
	TLSConfig *tls.Config

	// Username / Password — optional MQTT credentials.
	Username string
	Password []byte

	// WillMessage — optional Last Will and Testament configuration.
	WillMessage *WillConfig

	// ReconnectBackoffFunc maps reconnect attempt number to wait duration.
	// nil uses autopaho's default constant 10s backoff.
	ReconnectBackoffFunc func(attempt int) time.Duration

	// MaxReconnectAttempts bounds how many consecutive failed attempts to
	// re-establish a lost connection are made before the client gives up.
	// Zero or negative reconnects forever, which is both the default and the
	// behaviour before this field existed.
	//
	// Zero meaning unlimited is deliberate on two counts: it makes the field's
	// zero value the pre-existing behaviour, and it is what vinculum-bus's
	// AutoReconnector already means by the same number (its check is
	// `maxRetries > 0 && attempts >= maxRetries`). "Do not reconnect at all" is
	// therefore not expressible here — it is not expressible there either, and
	// inventing a second meaning for one number across two clients would be
	// worse than the missing capability.
	//
	// It governs *re*connection only. The initial connection made by Start is
	// retried indefinitely whatever this says, because a broker that is not up
	// yet at process start is an ordinary situation and failing there would turn
	// a slow dependency into a startup failure.
	//
	// Giving up is terminal and quiet: the client logs an error, the connection
	// manager shuts down, and Done is closed. Nothing restarts it and the
	// process keeps running, which mirrors what vinculum-bus's AutoReconnector
	// does when its own retry budget runs out. Publishes after that point fail
	// the way they do for any closed client.
	//
	// The counter resets on every successful connection, so it bounds a single
	// outage rather than the lifetime of the client.
	MaxReconnectAttempts int

	// OnConnect is called from within OnConnectionUp after subscriptions are
	// registered. It runs synchronously; keep it fast.
	OnConnect func(ctx context.Context)

	// OnDisconnect is called from within OnConnectionDown when the connection
	// drops unexpectedly. It runs synchronously; keep it fast.
	OnDisconnect func(ctx context.Context)

	// MeterProvider — optional OTel MeterProvider for connection metrics.
	MeterProvider metric.MeterProvider

	// Logger — optional logger.
	Logger *zap.Logger
}

// WillConfig holds Last Will and Testament configuration.
type WillConfig struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}
