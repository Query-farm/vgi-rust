# Iroh

The high-level client accepts both canonical endpoint forms when the `iroh`
feature is enabled:

```rust
let raw = vgi_client::VgiClient::connect_to("iroh://<endpoint-id>")?;
let http = vgi_client::VgiClient::connect_to("httpi://<endpoint-id>/vgi")?;
```

`VgiClient::connect_httpi_with_options` exposes the same operational controls
as the Python framework: a stable secret key, default/custom/disabled relays,
remote relay and direct-address hints, connect/I/O deadlines, bearer auth, and
the advertised decoded-response budget.

`connect_iroh_with_endpoint` accepts an owned Tokio runtime, application-built
`iroh::Endpoint`, `EndpointAddr`, and client options for private relays, stable
keys, and direct-address hints.

A non-Rust worker normally runs behind `vgi-iroh-bridge`. Rust workers expose
the same portable shape through `Worker::run`:

```console
worker --iroh-raw-upstream 127.0.0.1:9400 --iroh-issuer production
worker --http --iroh-issuer production
```

The raw listener requires the bridge's identity-bearing PROXY v2 preamble.
HTTP mode trusts only the configured exact bridge address. Repeat
`--iroh-trusted-proxy <IP>` to replace the loopback default or add
`--iroh-observe` to retain evidence without authenticating from it.
