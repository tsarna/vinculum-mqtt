package publisher

import (
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// PublisherBuilder constructs an MQTTPublisher with a fluent API.
type PublisherBuilder struct {
	clientName     string
	topicMappings  []TopicMapping
	defaultXform   DefaultTopicTransform
	defaultQoS     byte
	defaultRetain  bool
	wireFormat     wire.WireFormat
	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
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

// WithWireFormat sets the wire format used to serialize outbound payloads.
func (b *PublisherBuilder) WithWireFormat(f wire.WireFormat) *PublisherBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
func (b *PublisherBuilder) WithWireFormatName(name string) *PublisherBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithLogger sets the logger.
func (b *PublisherBuilder) WithLogger(l *zap.Logger) *PublisherBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// WithClientName sets the vinculum client name used in metric attributes.
func (b *PublisherBuilder) WithClientName(name string) *PublisherBuilder {
	b.clientName = name
	return b
}

// WithMeterProvider sets the OTel MeterProvider. nil disables metrics.
func (b *PublisherBuilder) WithMeterProvider(p metric.MeterProvider) *PublisherBuilder {
	b.meterProvider = p
	return b
}

// WithTracerProvider sets the OTel TracerProvider used to create send spans.
// When nil, the global TracerProvider is used.
func (b *PublisherBuilder) WithTracerProvider(tp trace.TracerProvider) *PublisherBuilder {
	b.tracerProvider = tp
	return b
}

// Build returns an MQTTPublisher. The publisher starts disconnected; call
// SetPublishFunc (or use client.MQTTClient.Start) before publishing events.
func (b *PublisherBuilder) Build() (*MQTTPublisher, error) {
	var meter metric.Meter
	if b.meterProvider != nil {
		meter = b.meterProvider.Meter("github.com/tsarna/vinculum-mqtt/publisher")
	}
	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}
	return &MQTTPublisher{
		topicMappings:  b.topicMappings,
		defaultXform:   b.defaultXform,
		defaultQoS:     b.defaultQoS,
		defaultRetain:  b.defaultRetain,
		wireFormat:     wf,
		logger:         b.logger,
		metrics:        NewPublisherMetrics(b.clientName, meter),
		tracerProvider: b.tracerProvider,
	}, nil
}
