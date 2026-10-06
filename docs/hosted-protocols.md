# Hosting additional protocols, and `vgi_rpc.Identity.v1`

A worker always hosts its own protocol, `vgi.v2` (what the DuckDB extension
speaks), and `vgi_rpc.Reflection.v1`. It can host more, on every transport.

## Additional application protocols: `Worker::hosted_protocols`

```rust,ignore
use vgi::Worker;
use vgi_rpc::server::HostedProtocol;

#[vgi_rpc::service]
impl Health {
    #[unary]
    fn ping(&self) -> vgi_rpc::Result<String> { Ok("pong".into()) }
}

let mut worker = Worker::new();
worker.hosted_protocols(|| {
    let mut health = HostedProtocol::new("example.Health.v1").with_version("1.0.0");
    Health::register_with(&mut health, std::sync::Arc::new(Health));
    vec![health]
});
```

- The hook is called **once**, when the worker builds its server. It may read
  configuration or the environment; what it returns is fixed for the life of
  the process, so reflection output and protocol hashes stay stable.
- The protocols are hosted on **every** transport the worker serves: stdio,
  `--unix`, `--tcp`, the Iroh raw upstream, `--http`, and the SAB/wasm path.
  `list_protocols` lists `vgi.v2` first, then these, in the order returned.
- Requests route on the pair `(protocol, method)`, with no fallback to
  `vgi.v2`, so an added protocol cannot change how DuckDB's calls dispatch --
  even if its method names repeat `vgi.v2`'s.
- Each protocol has its own optional version, gated only on its own calls.
- There is no way to host a *subset* of a protocol: the protocol is the unit of
  optionality. A capability that may be absent is its own protocol.
- A name that is malformed, repeated, equal to `vgi.v2`, or under the reserved
  `vgi_rpc.` prefix stops the worker at startup with an error naming
  `Worker::hosted_protocols`.

A method raises errors with the vgi-rpc error model: a canonical code, an
optional kind, and typed details, e.g.

```rust,ignore
return Err(vgi_rpc::RpcError::new("ReportRebuilding", "report is being rebuilt")
    .with_status(vgi_rpc::Code::Unavailable, "report_rebuilding")
    .with_details([vgi_rpc::ErrorDetail::retry_info(30.0)]));
```

## `vgi_rpc.Identity.v1`: `Worker::resolve_token` / `Worker::mint_grant`

Identity resolves an opaque bearer credential to a principal
(`introspect_token`) and mints standing grants for the calling user
(`issue_grant`). It is framework-owned, so it is not supplied through
`hosted_protocols`; a worker opts in by setting the hooks:

```rust,ignore
worker.resolve_token(|token| match api_keys::lookup(token) {
    Ok(Some(row)) => Ok(Some(TokenIdentity::new(row.principal).with_token_name(row.label))),
    Ok(None) => Ok(None),                      // the store answered: unknown
    Err(e) => Err(RpcError::auth_unavailable(e.to_string()).with_retry_after(10)),
});
worker.introspect_principals(["edge-proxy@example.com"]);
```

- **Absent unless set.** With neither hook, the protocol is not hosted at all.
  Only the methods whose hooks exist are hosted.
- **HTTP only**: it is hosted on the transport that authenticates callers,
  because its allowlist names principals.
- **An allowlist is required for `resolve_token`.** Set it with
  `Worker::introspect_principals` or `VGI_INTROSPECT_PRINCIPALS`
  (comma-separated). Without one the worker refuses to start over HTTP: there
  is no permissive default, because "any authenticated caller" lets any user
  resolve any other user's credential to its owner. A worker that only mints
  needs no allowlist.

### Which error to return for a transient failure

Return `Ok(None)` when the store answered and the credential is unknown. When
the answer is **not knowable** -- the store is down, a timeout, a 5xx --
return `RpcError::auth_unavailable(..)`, optionally with
`.with_retry_after(seconds)`. That is the same error an HTTP authenticator
returns for an outage, and vgi-rpc translates it from either hook into the
wire's `identity_unavailable` kind with code `UNAVAILABLE` and your retry hint
as `vgi_rpc.RetryInfo`. A caller can then tell an outage from "unknown":
it retries the first and may cache the second. Returning a plain error such as
`RpcError::value_error` instead would read as a definitive refusal. Never put
the credential or claims in the error.
