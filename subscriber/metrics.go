package subscriber

import (
	"context"
	"time"

	"github.com/tsarna/vinculum-bus/o11y"
)

// SubscriberMetrics holds the o11y instruments for an MQTTSubscriber.
// A nil *SubscriberMetrics is valid and results in no-op recording.
type SubscriberMetrics struct {
	messagesReceived o11y.Counter   // mqtt_subscriber_messages_received_total  (label: topic)
	errors           o11y.Counter   // mqtt_subscriber_errors_total             (label: topic)
	processDuration  o11y.Histogram // mqtt_subscriber_process_duration_seconds (label: topic)
}

// NewSubscriberMetrics creates a SubscriberMetrics using the given provider.
// Returns nil if provider is nil, which is safe to call all methods on.
func NewSubscriberMetrics(provider o11y.MetricsProvider) *SubscriberMetrics {
	if provider == nil {
		return nil
	}
	return &SubscriberMetrics{
		messagesReceived: provider.Counter("mqtt_subscriber_messages_received_total"),
		errors:           provider.Counter("mqtt_subscriber_errors_total"),
		processDuration:  provider.Histogram("mqtt_subscriber_process_duration_seconds"),
	}
}

// RecordReceived increments the received counter for the given MQTT topic.
func (m *SubscriberMetrics) RecordReceived(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.messagesReceived.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// RecordError increments the error counter for the given topic.
func (m *SubscriberMetrics) RecordError(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// RecordProcessDuration records how long OnEvent took for the given topic.
func (m *SubscriberMetrics) RecordProcessDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.processDuration.Record(ctx, d.Seconds(), o11y.Label{Key: "topic", Value: topic})
}
