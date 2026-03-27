# vinculum-mqtt

MQTT 5.0 client integration for [vinculum](https://github.com/tsarna/vinculum),
implemented using [paho.golang](https://github.com/eclipse/paho.golang) with
`autopaho` for automatic reconnection.

## Overview

A `client "mqtt"` block manages a single MQTT connection with zero or more
`publisher` sub-blocks (vinculum → MQTT broker) and zero or more `subscriber`
sub-blocks (MQTT broker → vinculum). All publishers and subscribers within one
block share the same connection.

## Quick Start

```hcl
client "mqtt" "iot" {
  brokers   = ["mqtt://localhost:1883"]
  client_id = "vinculum-iot"

  publisher "out" {
    qos = 1
  }

  subscriber "in" {
    subscriber = bus.main
    topic_subscription {
      mqtt_topic = "sensors/#"
    }
  }
}

# Route vinculum events to MQTT
subscription "to_mqtt" {
  target     = bus.main
  topics     = ["sensors/#"]
  subscriber = client.iot.publisher.out
}
```

## Connection Configuration

| Attribute | Type | Default | Description |
|---|---|---|---|
| `brokers` | `list(string)` | required | Broker URLs: `mqtt://`, `mqtts://`, `ws://`, `wss://` |
| `client_id` | expression | `"vinculum-<name>"` | MQTT client identifier — must be unique per connection |
| `keep_alive` | duration | `"30s"` | PINGREQ interval |
| `clean_start` | bool | `false` | Request a clean session on initial connection |
| `session_expiry_interval` | duration | `0` | How long the broker retains session state after disconnect (0 = until disconnect) |

### TLS

```hcl
tls {
  enabled              = true
  ca_cert              = "/etc/certs/ca.crt"
  cert                 = "/etc/certs/client.crt"   # optional, for mTLS
  key                  = "/etc/certs/client.key"   # optional, for mTLS
  insecure_skip_verify = false
}
```

### Authentication

```hcl
auth {
  username = "vinculum"
  password = env.MQTT_PASSWORD
}
```

### Reconnect

```hcl
reconnect {
  initial_delay  = "1s"
  max_delay      = "60s"
  backoff_factor = 2.0
}
```

If omitted, autopaho uses its own default backoff.

### Last Will and Testament

```hcl
will {
  topic   = "vinculum/status"
  payload = jsonencode({status = "offline"})
  qos     = 1
  retain  = true
}
```

`topic` and `payload` are HCL expressions evaluated once at config time. The
broker publishes the will on unexpected disconnect (network failure, process
kill). A graceful `Stop()` suppresses the will.

### Lifecycle Hooks

```hcl
on_connect    = send(ctx, bus.main, "mqtt/connected",    {client = "iot"})
on_disconnect = send(ctx, bus.main, "mqtt/disconnected", {client = "iot"})
```

`on_connect` fires after every connection or reconnection (after subscriptions
are registered). `on_disconnect` fires when the connection drops, before
reconnection. Both run synchronously — keep them fast.

## Publisher Block

```hcl
publisher "name" {
  qos    = 1      # default QoS for all publishes (0 or 1; default: 1)
  retain = false  # default retain flag (default: false)

  # Optional: per-pattern QoS/retain overrides and optional topic renames.
  topic_mapping {
    pattern = "alerts/#"
    qos     = 1
    retain  = true
  }
  topic_mapping {
    pattern    = "sensor/+deviceId/reading"
    mqtt_topic = "sensors/${ctx.fields.deviceId}/data"   # HCL expression
    qos        = 1
  }

  # When no topic_mapping matches:
  # "verbatim" — use vinculum topic verbatim at publisher-level QoS/retain (default)
  # "error"    — return an error
  # "ignore"   — silently drop
  default_topic_transform = "verbatim"
}
```

`mqtt_topic` is an optional HCL expression with `topic`, `msg`, and `fields` in scope.

### Addressing publishers in VCL

```hcl
subscriber = client.iot.publishers        # fan-out to all publishers
subscriber = client.iot.publisher.main    # specific named publisher
```

### Message serialization

| Payload type | Wire encoding |
|---|---|
| `cty.Value` | Converted via `go2cty2go`, then `json.Marshal` |
| `[]byte` | Passed through unchanged |
| other Go value | `json.Marshal` |

vinculum `fields` map to MQTT 5 user properties (one property per key).

## Subscriber Block

```hcl
subscriber "name" {
  subscriber = bus.main     # forward to a bus or subscriber
  # action = loginfo(ctx, "msg", {topic = ctx.topic})  # or a VCL expression

  qos              = 1      # default QoS for subscriptions (default: 0)
  handle_retained  = true   # deliver retained messages (default: true)
  shared_group     = ""     # MQTT 5 shared subscription group (like Kafka consumer group)

  topic_subscription {
    mqtt_topic     = "sensors/+deviceId/data"   # sent to broker as "sensors/+/data"
    vinculum_topic = "sensor/${fields.deviceId}/reading"  # HCL expression (default: mqtt topic verbatim)
    qos            = 1      # overrides subscriber-level qos
  }
}
```

`vinculum_topic` is an optional HCL expression with `ctx.topic` (the MQTT
topic), `msg`, and `fields` in scope. Omitting it uses the MQTT topic verbatim.

### Field extraction

Named wildcards like `+deviceId` in `mqtt_topic` extract the matched segment
into `fields["deviceId"]`. The broker subscription uses the plain `+` wildcard;
field extraction is handled locally.

### Shared subscriptions

When `shared_group` is set, subscriptions are sent as `$share/<group>/<topic>`.
The broker load-balances delivery across all clients with the same group — only
one instance receives each message. Use this when running multiple vinculum
instances to avoid duplicate processing (equivalent to Kafka consumer groups).

### Message deserialization

| Payload | Vinculum `msg` type |
|---|---|
| Valid JSON | `any` (`map[string]any`, `[]any`, etc.) |
| Non-JSON bytes | `[]byte` |

MQTT 5 user properties become `fields["key"] = "value"` (last value wins for
duplicates). If the message was retained, `fields["$retained"] = "true"` is
added.

## Observability

### Publisher metrics

| Metric | Type |
|---|---|
| `mqtt_publisher_messages_sent_total` | Counter |
| `mqtt_publisher_errors_total` | Counter |
| `mqtt_publisher_publish_duration_seconds` | Histogram |

### Subscriber metrics

| Metric | Type |
|---|---|
| `mqtt_subscriber_messages_received_total` | Counter |
| `mqtt_subscriber_errors_total` | Counter |
| `mqtt_subscriber_process_duration_seconds` | Histogram |

### Connection metrics

| Metric | Type | Description |
|---|---|---|
| `mqtt_client_connected` | Gauge | 1 = connected, 0 = not |
| `mqtt_client_reconnects_total` | Counter | Total reconnection events |

Configure metrics via the `metrics` attribute:

```hcl
client "mqtt" "iot" {
  ...
  metrics = server.metrics.default
}
```

## Pitfalls

**Client ID uniqueness.** Two connections with the same `client_id` cause the
broker to disconnect the older one. Use a unique ID per instance (e.g.
`"vinculum-iot-${sys.hostname}"`).

**Retained message burst on reconnect.** Every reconnect re-subscribes, and the
broker re-delivers retained messages. Use `handle_retained = false` if retained
messages are not needed, or filter with vinculum subscription transforms.

**`$SYS` broker topics.** Subscribing to `#` receives broker diagnostics under
`$SYS/`. Filter them with `drop_topic_pattern("$SYS/#")` in subscription
transforms.

**Will is not sent on graceful disconnect.** This is correct MQTT behavior.
Publish a goodbye message explicitly in `on_disconnect` if needed.

**QoS mismatch.** MQTT delivers at the lower of publisher and subscriber QoS.
Ensure consistency across your pipeline.

## License

BSD 2-Clause. See [LICENSE](LICENSE).
