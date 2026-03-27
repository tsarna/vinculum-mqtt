// Package client manages a single autopaho.ConnectionManager shared by all
// publishers and subscribers within a vinculum MQTT client block.
//
// # Usage
//
// Create a client, add publishers and subscribers, then start:
//
//	c, err := client.NewClient(client.ClientConfig{
//	    ServerURLs: []*url.URL{mustURL("mqtt://localhost:1883")},
//	    ClientID:   "my-client",
//	    KeepAlive:  30 * time.Second,
//	})
//	c.AddPublisher(pub)
//	c.AddSubscriber(sub)
//	if err := c.Start(ctx); err != nil { ... }
//	defer c.Stop(ctx)
//
// Start blocks until the first connection is established and all subscriptions
// are registered. Publishers receive their publish function during Start.
//
// # Connection lifecycle
//
// The underlying autopaho.ConnectionManager reconnects automatically on
// disconnect. On each reconnect, broker subscriptions are re-registered.
// Publisher publish functions remain valid across reconnects.
//
// Use Stop to send a graceful MQTT DISCONNECT and wait for the connection
// manager to shut down. Stop is safe to call before Start (no-op).
package client
