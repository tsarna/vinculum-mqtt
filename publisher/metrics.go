package publisher

import (
	"context"
	"time"

	"github.com/tsarna/vinculum-bus/o11y"
)

// PublisherMetrics holds the o11y instruments for an MQTTPublisher.
// A nil *PublisherMetrics is valid and results in no-op recording.
type PublisherMetrics struct {
	messagesSent    o11y.Counter   // mqtt_publisher_messages_sent_total          (label: topic)
	errors          o11y.Counter   // mqtt_publisher_errors_total                 (label: topic)
	publishDuration o11y.Histogram // mqtt_publisher_publish_duration_seconds     (label: topic)
}

// NewPublisherMetrics creates a PublisherMetrics using the given provider.
// Returns nil if provider is nil, which is safe to call all methods on.
func NewPublisherMetrics(provider o11y.MetricsProvider) *PublisherMetrics {
	if provider == nil {
		return nil
	}
	return &PublisherMetrics{
		messagesSent:    provider.Counter("mqtt_publisher_messages_sent_total"),
		errors:          provider.Counter("mqtt_publisher_errors_total"),
		publishDuration: provider.Histogram("mqtt_publisher_publish_duration_seconds"),
	}
}

// RecordSent increments the sent counter for the given MQTT topic.
func (m *PublisherMetrics) RecordSent(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.messagesSent.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// RecordError increments the error counter for the given topic.
func (m *PublisherMetrics) RecordError(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// RecordPublishDuration records how long the publish call took.
func (m *PublisherMetrics) RecordPublishDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.publishDuration.Record(ctx, d.Seconds(), o11y.Label{Key: "topic", Value: topic})
}
