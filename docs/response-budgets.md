# Producer response budgets

HTTP hosting can impose an application limit, a deployment/platform limit,
and a client-advertised accepted maximum. `vgi-rpc` intersects those values
into one hard per-request response limit and exposes an advisory batching
target clamped to it.

`TableProducer::next_batch` receives these snapshots on its existing
`OutputCollector` argument:

```rust,ignore
let hard = out.response_limit_bytes();
let target = out.preferred_response_bytes().or(hard);
```

The values describe the complete decoded, uncompressed Arrow IPC response,
including framing and metadata, not the compressed HTTP entity body. A
producer should leave framing/metadata headroom and may emit less.
They are hints for choosing a batch size; the transport remains the authority
and returns a structured exception without a continuation cursor if the final
body crosses the hard limit. A producer must remain correct when both values
are `None` and on non-HTTP transports.
