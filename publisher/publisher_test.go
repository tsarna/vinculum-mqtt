package publisher

import (
	"context"
	"errors"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zclconf/go-cty/cty"
)

// capturePublish records publish calls without a broker.
type capturePublish struct {
	calls []*paho.Publish
	resp  *paho.PublishResponse
	err   error
}

func (c *capturePublish) publish(ctx context.Context, pub *paho.Publish) (*paho.PublishResponse, error) {
	c.calls = append(c.calls, pub)
	return c.resp, c.err
}

func makePublisher(mappings []TopicMapping, defaultXform DefaultTopicTransform, defaultQoS byte) *MQTTPublisher {
	return &MQTTPublisher{
		topicMappings: mappings,
		defaultXform:  defaultXform,
		defaultQoS:    defaultQoS,
	}
}

// --- serializePayload ---

func TestSerializePayload_Nil(t *testing.T) {
	b, err := serializePayload(nil)
	require.NoError(t, err)
	assert.Nil(t, b)
}

func TestSerializePayload_BytesPassthrough(t *testing.T) {
	raw := []byte(`{"already":"encoded"}`)
	b, err := serializePayload(raw)
	require.NoError(t, err)
	assert.Equal(t, raw, b)
}

func TestSerializePayload_GoValue(t *testing.T) {
	b, err := serializePayload(map[string]any{"hello": "world"})
	require.NoError(t, err)
	assert.Equal(t, `{"hello":"world"}`, string(b))
}

func TestSerializePayload_CtyString(t *testing.T) {
	b, err := serializePayload(cty.StringVal("hello"))
	require.NoError(t, err)
	assert.Equal(t, `"hello"`, string(b))
}

func TestSerializePayload_CtyObject(t *testing.T) {
	val := cty.ObjectVal(map[string]cty.Value{
		"count": cty.NumberIntVal(42),
	})
	b, err := serializePayload(val)
	require.NoError(t, err)
	assert.Equal(t, `{"count":42}`, string(b))
}

func TestSerializePayload_CtyNumber(t *testing.T) {
	b, err := serializePayload(cty.NumberIntVal(99))
	require.NoError(t, err)
	assert.Equal(t, `99`, string(b))
}

// --- fieldsToUserProperties ---

func TestFieldsToUserProperties_Nil(t *testing.T) {
	assert.Nil(t, fieldsToUserProperties(nil))
}

func TestFieldsToUserProperties_Empty(t *testing.T) {
	assert.Nil(t, fieldsToUserProperties(map[string]string{}))
}

func TestFieldsToUserProperties_Single(t *testing.T) {
	props := fieldsToUserProperties(map[string]string{"k": "v"})
	require.Len(t, props, 1)
	assert.Equal(t, "k", props[0].Key)
	assert.Equal(t, "v", props[0].Value)
}

func TestFieldsToUserProperties_Multiple(t *testing.T) {
	fields := map[string]string{"a": "1", "b": "2"}
	props := fieldsToUserProperties(fields)
	assert.Len(t, props, 2)
	// Convert to map for order-independent comparison.
	got := make(map[string]string, len(props))
	for _, p := range props {
		got[p.Key] = p.Value
	}
	assert.Equal(t, fields, got)
}

// --- resolveMapping ---

func TestResolveMapping_ExactMatch(t *testing.T) {
	p := makePublisher([]TopicMapping{
		{Pattern: "sensor/temp", QoS: 1, Retain: false},
	}, DefaultTopicError, 0)

	topic, qos, retain, err := p.resolveMapping("sensor/temp", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "sensor/temp", topic)
	assert.Equal(t, byte(1), qos)
	assert.False(t, retain)
}

func TestResolveMapping_FirstMatchWins(t *testing.T) {
	p := makePublisher([]TopicMapping{
		{Pattern: "sensor/#", QoS: 0},
		{Pattern: "sensor/temp", QoS: 1},
	}, DefaultTopicError, 0)

	_, qos, _, err := p.resolveMapping("sensor/temp", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, byte(0), qos, "first match should win")
}

func TestResolveMapping_RetainFlag(t *testing.T) {
	p := makePublisher([]TopicMapping{
		{Pattern: "status/#", QoS: 1, Retain: true},
	}, DefaultTopicError, 0)

	_, _, retain, err := p.resolveMapping("status/device1", nil, nil)
	require.NoError(t, err)
	assert.True(t, retain)
}

func TestResolveMapping_MQTTTopicFunc(t *testing.T) {
	p := makePublisher([]TopicMapping{
		{
			Pattern: "sensor/+deviceId/reading",
			MQTTTopicFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				return "devices/" + fields["deviceId"] + "/telemetry", nil
			},
			QoS: 1,
		},
	}, DefaultTopicError, 0)

	topic, _, _, err := p.resolveMapping("sensor/abc/reading", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "devices/abc/telemetry", topic)
}

func TestResolveMapping_MQTTTopicFuncEmpty_UsesVinculumTopic(t *testing.T) {
	p := makePublisher([]TopicMapping{
		{
			Pattern: "sensor/#",
			MQTTTopicFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				return "", nil // returning "" means use vinculum topic
			},
		},
	}, DefaultTopicError, 0)

	topic, _, _, err := p.resolveMapping("sensor/temp", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "sensor/temp", topic)
}

func TestResolveMapping_MQTTTopicFuncError(t *testing.T) {
	p := makePublisher([]TopicMapping{
		{
			Pattern: "sensor/#",
			MQTTTopicFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				return "", errors.New("resolution failed")
			},
		},
	}, DefaultTopicError, 0)

	_, _, _, err := p.resolveMapping("sensor/temp", nil, nil)
	assert.Error(t, err)
}

func TestResolveMapping_FieldExtraction(t *testing.T) {
	var capturedFields map[string]string
	p := makePublisher([]TopicMapping{
		{
			Pattern: "sensor/+deviceId/reading",
			MQTTTopicFunc: func(topic string, msg any, fields map[string]string) (string, error) {
				capturedFields = fields
				return topic, nil
			},
		},
	}, DefaultTopicError, 0)

	provided := map[string]string{"region": "us-east", "deviceId": "should-be-overridden"}
	_, _, _, err := p.resolveMapping("sensor/abc123/reading", nil, provided)
	require.NoError(t, err)
	assert.Equal(t, "abc123", capturedFields["deviceId"], "pattern-extracted field should override provided")
	assert.Equal(t, "us-east", capturedFields["region"], "provided field should be preserved")
}

func TestResolveMapping_VerbatimDefault(t *testing.T) {
	p := makePublisher(nil, DefaultTopicVerbatim, 1)

	topic, qos, _, err := p.resolveMapping("any/topic/here", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "any/topic/here", topic)
	assert.Equal(t, byte(1), qos)
}

func TestResolveMapping_IgnoreDefault(t *testing.T) {
	p := makePublisher(nil, DefaultTopicIgnore, 0)

	topic, _, _, err := p.resolveMapping("any/topic", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "", topic, "ignore should return empty topic")
}

func TestResolveMapping_ErrorDefault(t *testing.T) {
	p := makePublisher(nil, DefaultTopicError, 0)

	_, _, _, err := p.resolveMapping("any/topic", nil, nil)
	assert.Error(t, err)
}

// --- OnEvent ---

func TestOnEvent_BeforeSetPublishFunc(t *testing.T) {
	p, err := NewPublisher().Build()
	require.NoError(t, err)

	err = p.OnEvent(context.Background(), "test/topic", nil, nil)
	assert.ErrorContains(t, err, "not yet connected")
}

func TestOnEvent_PublishFuncCalled(t *testing.T) {
	cap := &capturePublish{resp: &paho.PublishResponse{}}
	p, err := NewPublisher().Build()
	require.NoError(t, err)
	p.SetPublishFunc(cap.publish)

	err = p.OnEvent(context.Background(), "test/topic", map[string]any{"value": 42}, nil)
	require.NoError(t, err)
	require.Len(t, cap.calls, 1)
	assert.Equal(t, "test/topic", cap.calls[0].Topic)
	assert.Equal(t, `{"value":42}`, string(cap.calls[0].Payload))
}

func TestOnEvent_FieldsBecomeUserProperties(t *testing.T) {
	cap := &capturePublish{resp: &paho.PublishResponse{}}
	p, err := NewPublisher().Build()
	require.NoError(t, err)
	p.SetPublishFunc(cap.publish)

	fields := map[string]string{"region": "eu", "version": "2"}
	err = p.OnEvent(context.Background(), "test/topic", nil, fields)
	require.NoError(t, err)
	require.Len(t, cap.calls, 1)
	require.NotNil(t, cap.calls[0].Properties)
	got := make(map[string]string)
	for _, up := range cap.calls[0].Properties.User {
		got[up.Key] = up.Value
	}
	assert.Equal(t, fields, got)
}

func TestOnEvent_EmptyFieldsNoProperties(t *testing.T) {
	cap := &capturePublish{resp: &paho.PublishResponse{}}
	p, err := NewPublisher().Build()
	require.NoError(t, err)
	p.SetPublishFunc(cap.publish)

	err = p.OnEvent(context.Background(), "test/topic", nil, nil)
	require.NoError(t, err)
	require.Len(t, cap.calls, 1)
	assert.Nil(t, cap.calls[0].Properties, "no fields should mean no properties")
}

func TestOnEvent_IgnoreDefault_PublishFuncNotCalled(t *testing.T) {
	cap := &capturePublish{}
	p, err := NewPublisher().WithDefaultTransform(DefaultTopicIgnore).Build()
	require.NoError(t, err)
	p.SetPublishFunc(cap.publish)

	err = p.OnEvent(context.Background(), "test/topic", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, cap.calls, "publish should not be called for ignored topics")
}

func TestOnEvent_PublishFuncError(t *testing.T) {
	cap := &capturePublish{err: errors.New("broker unavailable")}
	p, err := NewPublisher().Build()
	require.NoError(t, err)
	p.SetPublishFunc(cap.publish)

	err = p.OnEvent(context.Background(), "test/topic", nil, nil)
	assert.ErrorContains(t, err, "broker unavailable")
}

func TestOnEvent_QoSFromMapping(t *testing.T) {
	cap := &capturePublish{resp: &paho.PublishResponse{}}
	p, err := NewPublisher().
		WithTopicMapping(TopicMapping{Pattern: "status/#", QoS: 0, Retain: true}).
		Build()
	require.NoError(t, err)
	p.SetPublishFunc(cap.publish)

	err = p.OnEvent(context.Background(), "status/device1", nil, nil)
	require.NoError(t, err)
	require.Len(t, cap.calls, 1)
	assert.Equal(t, byte(0), cap.calls[0].QoS)
	assert.True(t, cap.calls[0].Retain)
}
