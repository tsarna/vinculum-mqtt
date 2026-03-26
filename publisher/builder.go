package publisher

import (
	"github.com/tsarna/vinculum-bus/o11y"
	"go.uber.org/zap"
)

// PublisherBuilder constructs an MQTTPublisher with a fluent API.
type PublisherBuilder struct {
	topicMappings   []TopicMapping
	defaultXform    DefaultTopicTransform
	defaultQoS      byte
	defaultRetain   bool
	logger          *zap.Logger
	metricsProvider o11y.MetricsProvider
}

// NewPublisher returns a PublisherBuilder with default settings:
// default_topic_transform=verbatim, qos=1, retain=false.
func NewPublisher() *PublisherBuilder {
	return &PublisherBuilder{
		defaultXform:  DefaultTopicVerbatim,
		defaultQoS:    1,
		defaultRetain: false,
		logger:        zap.NewNop(),
	}
}

// WithTopicMapping appends a topic mapping. Mappings are evaluated in order;
// first match wins.
func (b *PublisherBuilder) WithTopicMapping(tm TopicMapping) *PublisherBuilder {
	b.topicMappings = append(b.topicMappings, tm)
	return b
}

// WithDefaultTransform sets the fallback behaviour when no topic_mapping matches.
func (b *PublisherBuilder) WithDefaultTransform(t DefaultTopicTransform) *PublisherBuilder {
	b.defaultXform = t
	return b
}

// WithDefaultQoS sets the QoS level used when no topic_mapping matches and
// the default transform is DefaultTopicVerbatim.
func (b *PublisherBuilder) WithDefaultQoS(qos byte) *PublisherBuilder {
	b.defaultQoS = qos
	return b
}

// WithDefaultRetain sets the retain flag used when no topic_mapping matches
// and the default transform is DefaultTopicVerbatim.
func (b *PublisherBuilder) WithDefaultRetain(retain bool) *PublisherBuilder {
	b.defaultRetain = retain
	return b
}

// WithLogger sets the logger.
func (b *PublisherBuilder) WithLogger(l *zap.Logger) *PublisherBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// WithMetricsProvider sets the metrics provider. nil disables metrics.
func (b *PublisherBuilder) WithMetricsProvider(p o11y.MetricsProvider) *PublisherBuilder {
	b.metricsProvider = p
	return b
}

// Build returns an MQTTPublisher. The publisher starts disconnected; call
// SetPublishFunc (or use client.MQTTClient.Start) before publishing events.
func (b *PublisherBuilder) Build() (*MQTTPublisher, error) {
	return &MQTTPublisher{
		topicMappings: b.topicMappings,
		defaultXform:  b.defaultXform,
		defaultQoS:    b.defaultQoS,
		defaultRetain: b.defaultRetain,
		logger:        b.logger,
		metrics:       NewPublisherMetrics(b.metricsProvider),
	}, nil
}
