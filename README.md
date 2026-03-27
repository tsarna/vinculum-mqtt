# vinculum-mqtt

MQTT 5.0 client packages for [vinculum](https://github.com/tsarna/vinculum),
implemented using [paho.golang](https://github.com/eclipse/paho.golang) with
`autopaho` for automatic reconnection.

## Packages

### `publisher`

`MQTTPublisher` implements `bus.Subscriber`. It serializes vinculum events and
publishes them to an MQTT broker via a shared connection.

- Per-pattern topic mappings (first match wins) with optional QoS, retain, and
  topic rename per pattern.
- Default transform when no mapping matches: verbatim, error, or ignore.
- Payload serialization: `cty.Value` → `go2cty2go` → JSON; `[]byte`
  passthrough; other → JSON.
- vinculum `fields` encoded as MQTT 5 user properties.
- Metrics: messages sent, errors, publish duration.

```go
p, err := publisher.NewPublisher().
    WithDefaultQoS(1).
    WithTopicMapping(publisher.TopicMapping{
        Pattern: "alerts/#",
        QoS:     1,
        Retain:  true,
    }).
    Build()
```

`SetPublishFunc` must be called before `OnEvent` — `client.MQTTClient` does
this automatically during `Start`.

### `subscriber`

`MQTTSubscriber` receives MQTT messages and dispatches them as vinculum events
to a `bus.Subscriber`.

- Named wildcard field extraction: `+deviceId` in a pattern extracts the
  matched segment into `fields["deviceId"]`.
- Optional `VinculumTopicFunc` for dynamic vinculum topic mapping.
- Retained message handling: configurable `handleRetained` flag; retained
  messages add `fields["$retained"] = "true"`.
- MQTT 5 user properties → vinculum `fields` (last-value-wins for duplicates).
- Shared subscription support (`$share/<group>/<topic>`).
- Metrics: messages received, errors, process duration.

```go
s, err := subscriber.NewSubscriber().
    WithSubscription(subscriber.TopicSubscription{
        MQTTPattern: "sensors/+deviceId/data",
        QoS:         1,
    }).
    WithSubscriber(myBusSubscriber).
    Build()
```

### `client`

`MQTTClient` wraps an `autopaho.ConnectionManager` and manages the shared
connection lifecycle for a set of publishers and subscribers.

- All publishers and subscribers share one connection.
- `Start(ctx)` blocks until the first connection is established and all
  subscriptions are registered. Publishers receive their publish function during
  `Start`.
- Re-subscribes automatically on every reconnect.
- Configurable reconnect backoff, TLS, credentials, LWT, and lifecycle
  callbacks.
- Metrics: connected gauge, reconnect counter.

```go
c, err := client.NewClient(client.ClientConfig{
    ServerURLs: []*url.URL{u},
    ClientID:   "my-client",
    KeepAlive:  30 * time.Second,
})
c.AddPublisher(pub)
c.AddSubscriber(sub)
if err := c.Start(ctx); err != nil { ... }
defer c.Stop(ctx)
```

## VCL configuration

When used via vinculum, these packages are wired by `client "mqtt"` blocks in
VCL config files. See the
[vinculum MQTT client documentation](https://github.com/tsarna/vinculum/blob/main/doc/client-mqtt.md).

## License

BSD 2-Clause. See [LICENSE](LICENSE).
