package publisher

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PublisherMetrics holds the OTel instruments for an MQTTPublisher.
// A nil *PublisherMetrics is valid and results in no-op recording.
type PublisherMetrics struct {
	messagesSent    metric.Int64Counter     // messaging.client.sent.messages
	errors          metric.Int64Counter     // mqtt.publisher.errors
	publishDuration metric.Float64Histogram // messaging.client.operation.duration
	clientTag       attribute.KeyValue
}

// NewPublisherMetrics creates a PublisherMetrics using the given Meter.
// Returns nil if meter is nil, which is safe to call all methods on.
func NewPublisherMetrics(clientName string, meter metric.Meter) *PublisherMetrics {
	if meter == nil {
		return nil
	}
	ms, _ := meter.Int64Counter("messaging.client.sent.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Messages sent by the MQTT publisher"),
	)
	e, _ := meter.Int64Counter("mqtt.publisher.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("Errors encountered during MQTT publish"),
	)
	pd, _ := meter.Float64Histogram("messaging.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of MQTT publish operations"),
	)
	return &PublisherMetrics{
		messagesSent:    ms,
		errors:          e,
		publishDuration: pd,
		clientTag:       attribute.String("vinculum.client.name", clientName),
	}
}

// topicAttr returns standard messaging attributes for an MQTT topic.
func topicAttr(topic string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("messaging.system", "mqtt"),
		attribute.String("messaging.destination.name", topic),
	)
}

// RecordSent increments the sent counter for the given MQTT topic.
func (m *PublisherMetrics) RecordSent(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.messagesSent.Add(ctx, 1, topicAttr(topic),
		metric.WithAttributes(attribute.String("messaging.operation.name", "send")),
		metric.WithAttributes(m.clientTag),
	)
}

// RecordError increments the error counter for the given topic.
func (m *PublisherMetrics) RecordError(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, topicAttr(topic), metric.WithAttributes(m.clientTag))
}

// RecordPublishDuration records how long the publish call took.
func (m *PublisherMetrics) RecordPublishDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.publishDuration.Record(ctx, d.Seconds(), topicAttr(topic),
		metric.WithAttributes(attribute.String("messaging.operation.name", "send")),
		metric.WithAttributes(m.clientTag),
	)
}
