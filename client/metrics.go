package client

import (
	"context"

	"github.com/tsarna/vinculum-bus/o11y"
)

// ClientMetrics holds the o11y instruments for an MQTTClient.
// A nil *ClientMetrics is valid and results in no-op recording.
type ClientMetrics struct {
	connected  o11y.Gauge   // mqtt_client_connected        (1 = connected, 0 = not)
	reconnects o11y.Counter // mqtt_client_reconnects_total
}

// NewClientMetrics creates a ClientMetrics using the given provider.
// Returns nil if provider is nil, which is safe to call all methods on.
func NewClientMetrics(provider o11y.MetricsProvider) *ClientMetrics {
	if provider == nil {
		return nil
	}
	return &ClientMetrics{
		connected:  provider.Gauge("mqtt_client_connected"),
		reconnects: provider.Counter("mqtt_client_reconnects_total"),
	}
}

// SetConnected sets the connected gauge to 1 (connected) or 0 (not connected).
func (m *ClientMetrics) SetConnected(ctx context.Context, up bool) {
	if m == nil {
		return
	}
	v := 0.0
	if up {
		v = 1.0
	}
	m.connected.Set(ctx, v)
}

// IncrReconnects increments the reconnection counter.
func (m *ClientMetrics) IncrReconnects(ctx context.Context) {
	if m == nil {
		return
	}
	m.reconnects.Add(ctx, 1)
}
