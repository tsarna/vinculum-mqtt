package subscriber

import (
	"errors"

	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// SubscriberBuilder constructs an MQTTSubscriber with a fluent API.
type SubscriberBuilder struct {
	subscriptions  []TopicSubscription
	subscriber     bus.Subscriber
	handleRetained bool
	sharedGroup    string
	wireFormat     wire.WireFormat
	logger         *zap.Logger
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
}

// NewSubscriber returns a SubscriberBuilder with default settings:
// handle_retained=true.
func NewSubscriber() *SubscriberBuilder {
	return &SubscriberBuilder{
		handleRetained: true,
		logger:         zap.NewNop(),
	}
}

// WithSubscription appends a topic subscription. BrokerTopic is computed
// automatically from MQTTPattern if left empty.
func (b *SubscriberBuilder) WithSubscription(sub TopicSubscription) *SubscriberBuilder {
	if sub.BrokerTopic == "" {
		sub.BrokerTopic = stripFieldNames(sub.MQTTPattern)
	}
	b.subscriptions = append(b.subscriptions, sub)
	return b
}

// WithSubscriber sets the vinculum bus.Subscriber that receives dispatched events (required).
func (b *SubscriberBuilder) WithSubscriber(s bus.Subscriber) *SubscriberBuilder {
	b.subscriber = s
	return b
}

// WithHandleRetained controls whether retained messages are delivered (default true).
func (b *SubscriberBuilder) WithHandleRetained(v bool) *SubscriberBuilder {
	b.handleRetained = v
	return b
}

// WithSharedGroup sets the MQTT 5 shared subscription group name. When set,
// broker subscriptions are prefixed with "$share/<group>/".
func (b *SubscriberBuilder) WithSharedGroup(group string) *SubscriberBuilder {
	b.sharedGroup = group
	return b
}

// WithWireFormat sets the wire format used to deserialize inbound payloads.
func (b *SubscriberBuilder) WithWireFormat(f wire.WireFormat) *SubscriberBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
func (b *SubscriberBuilder) WithWireFormatName(name string) *SubscriberBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithLogger sets the logger.
func (b *SubscriberBuilder) WithLogger(l *zap.Logger) *SubscriberBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// WithMeterProvider sets the OTel MeterProvider. nil disables metrics.
func (b *SubscriberBuilder) WithMeterProvider(p metric.MeterProvider) *SubscriberBuilder {
	b.meterProvider = p
	return b
}

// WithTracerProvider sets the OTel TracerProvider used to create processing
// spans. When nil, the global TracerProvider is used.
func (b *SubscriberBuilder) WithTracerProvider(tp trace.TracerProvider) *SubscriberBuilder {
	b.tracerProvider = tp
	return b
}

// Build validates configuration and returns an MQTTSubscriber.
func (b *SubscriberBuilder) Build() (*MQTTSubscriber, error) {
	if b.subscriber == nil {
		return nil, errors.New("mqtt subscriber: subscriber is required")
	}
	if len(b.subscriptions) == 0 {
		return nil, errors.New("mqtt subscriber: at least one topic subscription is required")
	}
	var meter metric.Meter
	if b.meterProvider != nil {
		meter = b.meterProvider.Meter("github.com/tsarna/vinculum-mqtt/subscriber")
	}
	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}
	return &MQTTSubscriber{
		subscriptions:  b.subscriptions,
		subscriber:     b.subscriber,
		handleRetained: b.handleRetained,
		sharedGroup:    b.sharedGroup,
		wireFormat:     wf,
		logger:         b.logger,
		metrics:        NewSubscriberMetrics(meter),
		tracerProvider: b.tracerProvider,
	}, nil
}
