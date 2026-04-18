package client

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ClientMetrics holds the OTel instruments for an MQTTClient.
// A nil *ClientMetrics is valid and results in no-op recording.
type ClientMetrics struct {
	connected  metric.Float64Gauge // mqtt.client.connected (1 = connected, 0 = not)
	reconnects metric.Int64Counter // mqtt.client.reconnections
	clientTag  attribute.KeyValue
}

// NewClientMetrics creates a ClientMetrics using the given Meter.
// Returns nil if meter is nil, which is safe to call all methods on.
func NewClientMetrics(clientName string, meter metric.Meter) *ClientMetrics {
	if meter == nil {
		return nil
	}
	c, _ := meter.Float64Gauge("mqtt.client.connected",
		metric.WithUnit("1"),
		metric.WithDescription("MQTT client connection status (1=connected, 0=disconnected)"),
	)
	r, _ := meter.Int64Counter("mqtt.client.reconnections",
		metric.WithUnit("{reconnection}"),
		metric.WithDescription("MQTT client reconnection attempts"),
	)
	return &ClientMetrics{
		connected:  c,
		reconnects: r,
		clientTag:  attribute.String("vinculum.client.name", clientName),
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
	m.connected.Record(ctx, v, metric.WithAttributes(attribute.String("messaging.system", "mqtt"), m.clientTag))
}

// IncrReconnects increments the reconnection counter.
func (m *ClientMetrics) IncrReconnects(ctx context.Context) {
	if m == nil {
		return
	}
	m.reconnects.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.system", "mqtt"), m.clientTag))
}
