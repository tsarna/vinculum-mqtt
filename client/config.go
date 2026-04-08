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
