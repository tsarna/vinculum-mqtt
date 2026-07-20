package subscriber

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SubscriberMetrics holds the OTel instruments for an MQTTSubscriber.
// A nil *SubscriberMetrics is valid and results in no-op recording.
type SubscriberMetrics struct {
	messagesReceived metric.Int64Counter     // messaging.client.consumed.messages
	errors           metric.Int64Counter     // mqtt.subscriber.errors
	processDuration  metric.Float64Histogram // messaging.process.duration
	clientTag        attribute.KeyValue
}

// NewSubscriberMetrics creates a SubscriberMetrics using the given Meter.
// Returns nil if meter is nil, which is safe to call all methods on.
func NewSubscriberMetrics(clientName string, meter metric.Meter) *SubscriberMetrics {
	if meter == nil {
		return nil
	}
	mr, _ := meter.Int64Counter("messaging.client.consumed.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Messages received by the MQTT subscriber"),
	)
	e, _ := meter.Int64Counter("mqtt.subscriber.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("Errors encountered during MQTT message processing"),
	)
	pd, _ := meter.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of MQTT message processing"),
	)
	return &SubscriberMetrics{
		messagesReceived: mr,
		errors:           e,
		processDuration:  pd,
		clientTag:        attribute.String("vinculum.client.name", clientName),
	}
}

// topicAttr returns standard messaging attributes for an MQTT topic.
func topicAttr(topic string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("messaging.system", "mqtt"),
		attribute.String("messaging.destination.name", topic),
	)
}

// RecordReceived increments the received counter for the given MQTT topic.
func (m *SubscriberMetrics) RecordReceived(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.messagesReceived.Add(ctx, 1, topicAttr(topic), metric.WithAttributes(m.clientTag))
}

// RecordError increments the error counter for the given topic. errType
// classifies the failure (e.g. "subscription", "deserialize") and is
// recorded as the error.type attribute; an empty errType omits it.
func (m *SubscriberMetrics) RecordError(ctx context.Context, topic, errType string) {
	if m == nil {
		return
	}
	attrs := []attribute.KeyValue{m.clientTag}
	if errType != "" {
		attrs = append(attrs, attribute.String("error.type", errType))
	}
	m.errors.Add(ctx, 1, topicAttr(topic), metric.WithAttributes(attrs...))
}

// RecordProcessDuration records how long OnEvent took for the given topic.
func (m *SubscriberMetrics) RecordProcessDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.processDuration.Record(ctx, d.Seconds(), topicAttr(topic), metric.WithAttributes(m.clientTag))
}
