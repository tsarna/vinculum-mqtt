# Changelog

## v0.1.0 (2026-03-26)

Initial release.

### Added

- `publisher` package: `MQTTPublisher` implementing `bus.Subscriber`, forwarding vinculum events to MQTT.
  - Per-pattern topic mapping with optional topic rename, QoS, and retain overrides.
  - `DefaultTopicTransform`: verbatim (default), error, or ignore for unmapped topics.
  - Payload serialization: `cty.Value` → JSON, `[]byte` passthrough, other → JSON.
  - Fields → MQTT 5 user properties.
  - Metrics: messages sent, errors, publish duration.

- `subscriber` package: `MQTTSubscriber` routing inbound MQTT messages to vinculum events.
  - Named wildcard field extraction (`+deviceId` → `fields["deviceId"]`).
  - Optional `VinculumTopicFunc` for dynamic vinculum topic mapping.
  - Retained message handling with `handle_retained` flag and `$retained` field.
  - MQTT 5 user properties → vinculum fields (last-value-wins for duplicates).
  - Shared subscription support via `$share/<group>/<topic>` prefix.
  - Metrics: messages received, errors, process duration.

- `client` package: `MQTTClient` wrapping `autopaho.ConnectionManager`.
  - Single shared connection for all publishers and subscribers.
  - `Start()` blocks until first connection and subscriptions are registered.
  - Re-subscribes automatically on every reconnect.
  - Configurable reconnect backoff, TLS, credentials, and LWT.
  - `on_connect` / `on_disconnect` lifecycle callbacks.
  - Metrics: connected gauge, reconnect counter.
