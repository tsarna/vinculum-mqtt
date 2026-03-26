package subscriber

import (
	"errors"

	bus "github.com/tsarna/vinculum-bus"
	"github.com/tsarna/vinculum-bus/o11y"
	"go.uber.org/zap"
)

// SubscriberBuilder constructs an MQTTSubscriber with a fluent API.
type SubscriberBuilder struct {
	subscriptions   []TopicSubscription
	subscriber      bus.Subscriber
	handleRetained  bool
	sharedGroup     string
	logger          *zap.Logger
	metricsProvider o11y.MetricsProvider
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

// WithLogger sets the logger.
func (b *SubscriberBuilder) WithLogger(l *zap.Logger) *SubscriberBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// WithMetricsProvider sets the metrics provider. nil disables metrics.
func (b *SubscriberBuilder) WithMetricsProvider(p o11y.MetricsProvider) *SubscriberBuilder {
	b.metricsProvider = p
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
	return &MQTTSubscriber{
		subscriptions:  b.subscriptions,
		subscriber:     b.subscriber,
		handleRetained: b.handleRetained,
		sharedGroup:    b.sharedGroup,
		logger:         b.logger,
		metrics:        NewSubscriberMetrics(b.metricsProvider),
	}, nil
}
