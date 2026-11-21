# Orchard hosted mode

`VGI_ORCHARD_HOSTED=1` runs a worker as an Orchard *Field Worker*. The hosted
base image sets it; you do not set it per deployment, and a worker needs no
code to support it: every binary built with the default `transport-http`
feature can run hosted. Callers present a *Pass*: a short-lived
[RFC 9068](https://www.rfc-editor.org/rfc/rfc9068) JWT access token that the
Orchard Front Desk issues for one canonical Desk Address. The worker verifies
every Pass locally and accepts no other credential. The full design is in the
extension repository's `docs/location-delegation.md`; the code is
[`vgi::hosted`](../vgi/src/hosted.rs), a port of vgi-python's `vgi.hosted`.

```bash
VGI_ORCHARD_HOSTED=1 \
VGI_JWT_ISSUER=https://auth.queryfarm.cloud \
VGI_JWT_AUDIENCE=https://desk.queryfarm.cloud/d/dep_123 \
VGI_JWT_JWKS_URI=https://auth.queryfarm.cloud/.well-known/jwks.json \
VGI_SIGNING_KEY=... \
VGI_HTTP_BIND=0.0.0.0:8080 \
./my-worker --http
```

## Startup fails closed

The worker prints an error and exits with status 1, before binding anything,
unless all three of `VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE` and
`VGI_JWT_JWKS_URI` are set. Unset and empty are both errors. Each takes
exactly one value: there is no comma splitting, and a value containing a comma
is refused. `VGI_SIGNING_KEY` (or `Worker::signing_key`) must be set and at
least 32 bytes: it seals opaque values and, in hosted mode, the HTTP stream
tokens, so every machine of a deployment and every restart must share it.

It also refuses to start when any other way in is configured:

- `VGI_BEARER_TOKENS` or `VGI_OPTIONAL_BEARER_TOKENS`;
- sealed-grant keys (`VGI_RPC_GRANT_KEYS`, `--grant-key` or
  `Worker::grant_keys`);
- `Worker::resolve_token` or `Worker::mint_grant`;
- `Worker::authenticate`, or an authenticate callback passed to
  `vgi::transport::serve_http*`;
- `VGI_OAUTH_RESOURCE` (a Field Worker never advertises `resource_metadata`);
- `VGI_PROXY_PROOF_MODE` other than `off`;
- any transport but `--http`: stdio, `--unix`, `--tcp`,
  `--iroh-raw-upstream`, and `--http` behind the Iroh bridge (`--iroh-issuer`);
- a `VGI_ORCHARD_HOSTED` value other than `1`/`0`/`true`/`false`/`yes`/`no`/`on`/`off`.

## Verification

- algorithm `ES256` only (`VGI_JWT_ALGORITHMS` overrides; `none` and `HS*`
  are always refused);
- JOSE header `typ` must be `at+jwt` (case-insensitive, `application/` prefix
  optional);
- `iss` must match exactly, and `aud` must be exactly one value (a string or a
  one-element list) that matches exactly;
- `exp` is required, and `exp` / `nbf` are checked with 60 s of clock skew;
- the JWKS is cached for at most 10 minutes, or less if its
  `Cache-Control: max-age` is shorter. When that time is up the keys are
  discarded. If the re-fetch fails, requests get a 503 with `Retry-After`
  until it succeeds; stale keys are never used;
- a Pass with an unknown `kid` triggers a JWKS re-fetch at most once every 60
  seconds. Between re-fetches it is simply rejected.

## Every request needs a Pass

Except `HEAD /health` (the Front Desk's liveness probe) and CORS preflights
(`OPTIONS` with `Origin` and `Access-Control-Request-Method`), which browsers
send without credentials. That includes `GET /health`, `OPTIONS` without
`Access-Control-Request-Method`, the landing and describe pages and unknown
paths. A request without a valid Pass, including one with no `Authorization`
header at all, gets:

```text
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer error="invalid_token"
VGI-Auth-Reason: missing_credential | invalid_credential | expired_credential
VGI-Supported-Encodings: ...        (and the other VGI capability headers)
```

There is no `resource_metadata` in the challenge. Authentication runs in the
outermost HTTP middleware, before the request body is read, size-checked or
decompressed. CORS is open (`Access-Control-Allow-Origin: *`, no
`Allow-Credentials`): the only credential is the `Authorization` header a
caller sets itself. The `Authorization` header is never logged, at any level.

## Claims in function code

A Pass authenticates the caller with `domain = "orchard"` and the Pass's `sub`
as the principal. Its claims reach function code with their JSON types through
`BindParams::auth` / `ProcessParams::auth`:

```rust
fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
    let auth = &params.auth;
    let org = auth.claim_value("org_id");                // Some(Value::String(..))
    let verified = auth.claim_value("email_verified");   // Some(Value::Bool(true))
    // ...
}
```

`AuthContext::claim_value` reads the typed value; `auth.claims` holds the same
claims as strings. A Pass carries `sub`, `kind`, `email`, `email_verified`,
`name`, `org_id`, `org_name`, `federated_iss`, `federated_sub`, `client_id`,
`iat`, `nbf`, `exp` and `jti`.

The example worker registers `pass_claims(x)` in hosted mode: it returns
`{"claims": {...}, "domain": "orchard", "principal": "<sub>"}` (the whole
verified claim set) with sorted keys. `vgi-client/tests/orchard_hosted.rs`
drives it end to end, and `ci/hosted-conformance.sh` runs vgi's SDK-neutral
harness (`test/hosted/run_hosted_conformance.py`) against it:

```sh
VGI_SRC=../vgi VGI_WORKER_BIN=target/debug/vgi-example-worker ci/hosted-conformance.sh
```
