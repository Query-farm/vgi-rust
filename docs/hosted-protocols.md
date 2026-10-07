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

## Sealed grants: `issue_grant` tokens accepted as bearers

`issue_grant` mints a standing delegation that automation later presents as an
ordinary `Authorization: Bearer` credential. Give the worker a **grant key** and
the framework does both halves -- no storage, no author code:

```bash
# 32 random bytes, standard base64. Comma-separate several: the first mints,
# all verify (rotation: add the new key first, drop the old after its grants expire).
export VGI_RPC_GRANT_KEYS="$(head -c 32 /dev/urandom | base64)"
export VGI_RPC_GRANT_AUDIENCE=reports-prod          # optional, default ""
export VGI_RPC_GRANT_MAX_TTL_SECONDS=86400          # optional, default 7 days
my-worker --http                                    # or: --grant-key <base64> (repeatable)
```

or in code, `worker.grant_keys(vgi_rpc::grants::GrantKeys::new(...)?)`.

- **Over HTTP only**, `vgi_rpc.Identity.v1` hosts `issue_grant`, minting sealed
  `vgig1.` tokens -- unless the worker sets its own `Worker::mint_grant`, which
  keeps priority. The caller must have authenticated recently (`auth_time`
  within `max_auth_age`); a grant-authenticated caller carries no `auth_time`,
  so **grants never mint grants**.
- The HTTP server **accepts the grants back**: a request with `Bearer <grant>`
  runs as the user it was minted for, with `domain = "grant"` and claims
  `grant_id`, `purpose` and `scopes` (a JSON array as text; decode with
  `vgi_rpc::auth::identity_bearer::grant_scopes`). A forged, tampered,
  wrong-key or wrong-audience grant is 401 (`invalid_credential`); one past
  its lifetime is 401 (`expired_credential`).
- **Not individually revocable.** Keep `VGI_RPC_GRANT_MAX_TTL_SECONDS` short
  and re-issue; removing a key revokes every grant it minted.
- A malformed key (not base64 of exactly 32 bytes, a duplicate) stops the
  worker at startup. No key: nothing changes.

`resolve_token` feeds authentication the same way: when a worker sets it, its
HTTP server also accepts any bearer the hook resolves (`domain = "token"`), after
the deployment's own authenticator and after sealed grants. `Ok(None)` is a
401; `RpcError::auth_unavailable` is a 503 with your `Retry-After`. The hook
never sees a grant, a JWS, a blank or an over-long token.

With either active, a request carrying an `Authorization` header that nothing
accepts is refused (401) -- including under `VGI_OPTIONAL_BEARER_TOKENS`, which
otherwise answers anonymous for unknown tokens. A request with no credential at
all is still anonymous.

## Attach tickets: `vgi.attach_tickets.v1`

A ticket seals a user's ATTACH -- catalog name, options (secret ones included),
version specs -- so a runner holding that user's **grant** can reattach later
without ever seeing an option. The grant says *who*; the ticket says *what*.
Normative spec: vgi-python `docs/protocol/vgi-attach-tickets.md`; the format is
pinned byte for byte by `vgi/testdata/attach_ticket_vectors.json`, which the
unit tests in `vgi::attach_ticket` consume.

```bash
export VGI_SIGNING_KEY='<any stable value>'           # or worker.signing_key(...)
export VGI_RPC_GRANT_KEYS="$(head -c 32 /dev/urandom | base64)"
my-worker --http
```

- **Hosting.** The protocol is hosted on HTTP only, and only when the worker has
  a signing key (`VGI_SIGNING_KEY` or `Worker::signing_key`) **and** can issue
  grants (grant keys, or `Worker::mint_grant`). Otherwise it is absent, which a
  client learns from `vgi_rpc.Reflection.v1` `list_protocols`. This SDK never
  generates a signing key, so every key is a configured one and tickets
  survive restarts; rotating it invalidates every ticket.
- **`seal_attach(SealAttachRequest) -> AttachTicket`** (version `1.0.0`): seals
  the *caller's* attach of `catalog_name` with `options` (the Arrow IPC options
  record, as `catalog_attach` carries it) into `vgia1.…` text. Anonymous callers
  get `action_denied`; no fresh login is needed. Options are checked against
  the catalog's declared attach options (unknown, missing-required, the
  reserved name, over 16 KiB, `ttl_seconds < 0` → `invalid_request` with one
  `BadRequest` violation each). The lifetime is capped at the grant maximum
  (`VGI_RPC_GRANT_MAX_TTL_SECONDS` / the grant keys'); `ttl_seconds = 0` asks
  for that maximum, and with none the ticket does not expire
  (`expires_at = +inf`).
- **Redemption.** A `catalog_attach` whose options contain `vgi_attach_ticket`
  (any letter case) is replaced, before routing and before any catalog code,
  by the attach the ticket seals: the sealed catalog name (the request's name
  is ignored), options and version specs, keeping the request's
  `client_capabilities`. Any other option beside the ticket is
  `invalid_request`. The ticket opens only under the caller's principal --
  `attach_ticket_invalid` otherwise, indistinguishable from a forgery -- and
  only within its lifetime (60 s skew; `attach_ticket_expired`). On a worker
  with no signing key (every transport but keyed HTTP) every ticket is
  `attach_ticket_invalid`.
- **Reserved name.** `vgi_attach_ticket` cannot be declared as an attach
  option: `serialize_attach_option_spec_with_flags` refuses it, and a worker
  whose catalogs advertise it anyway panics at startup.
- **Never logged.** Neither the ticket nor a restored option appears in an
  error message.

```sql
-- runner, authenticated by the user's grant
ATTACH 'anything' AS sales (TYPE vgi, LOCATION 'https://worker',
    bearer_token '<grant>', vgi_attach_ticket '<ticket>');
```

The example worker serves the cross-SDK `ticket_probe` catalog (`region`,
default `'us-east-1'`; `api_key`, required and secret; table `main.probe`
returning `region` and the first 12 hex characters of `sha256(api_key)`).
Under `VGI_OPTIONAL_BEARER_TOKENS` its test bearers count as fresh logins
(`auth_time` = now), so `vgi-test-alice` / `vgi-test-bob` can call
`issue_grant` directly. A secondary catalog that needs its attach options at
query time can do the same as `ticket_probe`: set
`CatalogModel::attach_payload` and read it back with
`vgi::catalog::secondary_attach_payload`.
