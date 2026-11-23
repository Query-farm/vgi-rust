# Changelog

## Unreleased

- **Orchard hosted mode with the blob vendor.** `blob-vendor` is now a default
  feature of `vgi`, so every HTTP worker image (vgi-starter-rust's included)
  externalizes results, vends `__upload_url__` grants with `object_id` and a
  `ref` sealed for the Pass principal, and advertises `VGI-Arguments-Ref-Bytes`
  when Orchard sets `VGI_BLOB_VENDOR_URL`. A hosted worker built without the
  feature refuses to start when the vendor is set, instead of serving without
  it. Hosted mode defaults `VGI_RPC_MAX_REQUEST_BYTES` to 32 MiB (advertised as
  `VGI-Max-Request-Bytes`), as vgi-python's does. New test
  `vgi-client/tests/orchard_hosted_vendor.rs` (the port of vgi-csharp's
  `OrchardHostedVendorTests`), run in CI's hosted-mode job against the fake
  vendor.

- **Protocol 3.4 large values** (vgi `docs/large-values-3.4.md`):
  - **Generated bind types.** `BindRequest`, `ArgumentsLocation`,
    `CopyFromContext`, `CopyToContext` and the six requests embedding
    `bind_call` (`InitRequest`, `TableFunctionCardinalityRequest`,
    `TableFunctionStatisticsRequest`, `TableFunctionDynamicToStringRequest`,
    `TableFunctionPlanRequest`, `TableFunctionProjectionRequest`) are now the
    generated records (`vgi_protocol::generated::protocol_types`, re-exported
    from `protocol::dtos`). `bind_call` is a raw-preserving
    `Nested<BindRequest>` that re-encodes as the bytes received, and
    `BindRequest.arguments` is `Option<Bytes>` beside the new
    `arguments_location`. Breaking for code that builds these structs.
    `CardinalityRequest` / `DynamicToStringRequest` remain as aliases;
    statistics now uses its own `TableFunctionStatisticsRequest`.
  - **Argument references.** A bind whose arguments travelled by reference
    (`arguments_location`) is resolved at each turn's entry through vgi-rpc
    (sealed ref, object cache, size and SHA-256 checks); exactly one of
    `arguments` / `arguments_location` is required (`invalid_bind_request`),
    and an expired object answers `arguments_ref_missing` with "this prepared
    statement's large argument has expired; prepare it again". An `--http`
    worker with the blob vendor sends `VGI-Arguments-Ref-Bytes` (default
    4 MiB, `VGI_ARGUMENTS_REF_BYTES`; `VGI_TEST_DISABLE_ARGUMENTS_REF=1` turns
    it off). The split-token fingerprint hashes a location's size and SHA-256,
    never the value or the sealed ref. Table buffering persists the reference.
  - **`init: StateRef`.** An HTTP stream's cursor is now `StreamCursor`: the
    stream's immutable init (`StreamInit`, carrying the `bind_call` exactly as
    received) as one vgi-rpc `StateRef`, plus the per-turn fields. A
    continuation resolves it, and the arguments, on its first turn. The
    former `ExchangeBlob`, which carried the raw arguments in every token, is
    gone.
  - **Opaque values beside `bind_call`.** The sealer no longer rewrites a
    nested `bind_call`; its attach / transaction values are opened into a
    per-call overlay, so plaintext never reaches a re-encoded request or a
    state token.
  - `--http` serves with `token_ttl` 21600 s (was vgi-rpc's 300 s) and the
    `cursor_too_large` cursor rule.
  - Example worker: `blob_param_batches`, `blob_param_streams`,
    `blob_param_scalar` and the `blob_param_table` catalog table.

- **Orchard hosted mode** (`VGI_ORCHARD_HOSTED=1`, `vgi::hosted`), ported from
  vgi-python: an `--http` worker accepts only Orchard Passes (ES256 RFC 9068
  access tokens checked against `VGI_JWT_ISSUER` / `VGI_JWT_AUDIENCE` /
  `VGI_JWT_JWKS_URI`, JWKS cached at most 10 minutes and never used stale),
  on every request except `HEAD /health` and CORS preflights, before the body
  is read; and refuses to start with any other credential, a short or missing
  `VGI_SIGNING_KEY`, or a transport other than `--http`. Part of the default
  `transport-http` feature (adds jsonwebtoken with aws-lc-rs, and ureq with
  rustls). See `docs/orchard-hosted-mode.md`.
- `BindParams::auth` / `ProcessParams::auth`: the caller's full
  `AuthContext` (domain, principal, claims with their JSON types), beside
  `auth_principal`. Breaking for code that builds `ProcessParams` with a
  struct literal.

- **vgi protocol 3.4.0** (breaking: exact major+minor match, so it pairs only
  with a 3.4 extension). No `vgi.v3` message changes; 3.4 marks the move to
  vgi-rpc's upload-URL protocol v2 and mandatory pointer digests:
  - `--http` with the blob vendor advertises `VGI-Upload-URL-Support: 2`. Each
    `__upload_url__/init` entry carries the body's size and SHA-256, and the
    vendor's sized `/v1/blobs/put` (purpose `rpc`) signs the PUT for exactly
    that body, so storage refuses anything else. The retired `count` request is
    refused with a 400.
  - A request pointer must carry `vgi_rpc.location.sha256`; the worker verifies
    the fetched bytes against it before decoding.
  - Requires the unreleased vgi-rpc-rust `protocol-3.4` branch (pinned with
    `[patch.crates-io]`, `TODO(release-wave)`).

- **Blob-vendor externalization** (feature `blob-vendor`; the example worker
  enables it). Port of vgi-python's `function_storage_blobs` and serve wiring:
  - `storage::blobs::ExternalizingStorage` wraps any backend in the v1 value
    envelope (`0x00||raw` / `0x01||u64be size||sha256||object_id`, byte-identical
    to Python and checked against the vgi extension's
    `test/vectors/storage_envelope_v1.json`). Values of at least
    `VGI_STORAGE_EXTERNALIZE_BYTES` (default 512 KiB) go to the blob vendor and
    are verified on read. `cloudflare-do` storage is always wrapped (1 MiB per
    value); other backends opt in with `VGI_STORAGE_EXTERNALIZE=1`. An unreadable
    blob fails the query with `storage blob <id> failed its integrity check: ...`
    / `storage blob <id> is missing: ...`.
  - `--http` with `VGI_BLOB_VENDOR_URL` externalizes results over
    `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES`, vends upload URLs, and advertises
    `VGI_RPC_MAX_UPLOAD_BYTES`. `VGI_RPC_MAX_REQUEST_BYTES` is now honoured on
    every HTTP worker.
  - The example worker serves the cross-SDK `blob_test` catalog
    (`docs/testing/blob-test-catalog.md`), and the integration workflow has a
    `vendor` lane: the http suite plus `test/sql/external_storage/*` against the
    fake blob vendor.

- **`PushdownFilters::describe()`**: one stable, sorted line per pushed
  predicate (`"col OP"`, `"col IsNull"`, `"col In"`, `"And(...)"`, otherwise
  `"<first column> V2Expression"`), byte-identical to vgi-python's fixture
  description, for tests and diagnostics that assert what reached the worker.
- The example worker serves **`data.planned_events`** (vgi-python 3cf1489): a
  `scan_planning = filtered` table with DATE, TIMESTAMPTZ, VARCHAR, BIGINT,
  DOUBLE and BOOLEAN columns over `planned_events_<day>.parquet`. It prunes days
  on `event_date` / `ts` statistics and writes every planning call's days and
  described filters to `planned_events_last_plan.json`, for vgi's
  `catalog/scan_planning_predicates*.test` and `table/comments.test`. CI pins
  the shared suite to vgi 91f8d63.

## 0.52.0 (2026-11-19)

- **Cloudflare Durable Object storage backend** (feature `cloudflare-do`).
  `VGI_WORKER_SHARED_STORAGE=cloudflare-do` with `VGI_CF_DO_URL` and
  `VGI_CF_DO_TOKEN` stores worker state in the
  `vgi-cloudflare-durable-object-storage` service the other SDKs use, so Rust
  workers on several hosts share state with no storage server to run.
  `vgi::storage::CfDoStorage` shards by storage scope and retries with a stable
  `attempt_id`. Selecting `cloudflare-do` in a build without the feature now
  fails at startup instead of falling back to per-machine storage.
- **Framework state in storage is sealed** (sealed framework payload v1, the
  cross-SDK format: HKDF-SHA256 of the signing key, or of the host-local key
  file on stdio/unix/TCP, then AES-256-GCM with the storage slot as AAD; see
  `vgi::storage::seal`). A buffered function's init context (schemas,
  arguments, resolved secrets, `COPY ... TO` path, attach scope), an
  aggregate's bind-time arguments and a streaming aggregate's session no
  longer sit in shared storage in plaintext, and a tampered, moved or unsealed
  record is an error instead of being trusted (a rewritten `COPY ... TO` path
  could previously redirect a writer's output with the caller's secrets).
  Executions are short-lived, so state written by an older release mid-query
  is not migrated: such a query fails and is re-run.
- **Fewer storage round trips.** The buffered init is one record (one write,
  was up to nine), read once per worker process and then served from a
  process-local cache: a buffered `process` / `combine` makes no framework
  reads after the first (was seven per batch, eight for `COPY ... TO`). The
  request's own `attach_opaque_data` and `schema_path` are used directly.
  Aggregate `update` / `combine` / `finalize` make two storage calls each
  (were two per group, three per merged pair, and one per group plus one).
- **`FunctionStorage::kv_get_many` / `kv_put_many`** (default: a loop of the
  single-key calls). Batched in the memory and SQLite backends (one lock / one
  transaction), `cloudflare-do` (`state_get_many` / `state_put_many`) and
  `http`, where new `KvGetMany` / `KvPutMany` / `AppendMany` ops (appended to
  `StorageOp`, so old tags are unchanged) make `append_many` one round trip
  too. `vgi-storage-server` serves them; a client talking to an older server
  falls back to single ops after the first 400.
- **Breaking (internal storage keys):** the framework no longer writes the
  `outsc` / `insc` / `bufargs` / `bufflags` / `bufattach` / `bufschema` /
  `bufsecrets` / `copytofmt` / `copytopath` / `aggargs` / `strm_pkc` /
  `strm_okc` / `strm_sos` keys. A function that read one directly from
  `params.storage` must use `BufferingParams` instead (e.g.
  `params.arguments.named_bool("logging")` for the old `bufflags`, as the
  example worker now does).
- Missing or undecodable framework state is an error, not a silent default
  (empty arguments, empty secrets, a guessed output schema). The `http`
  backend no longer turns a reply of the wrong shape into an empty value.

## 0.51.1 (2026-11-16)

- **A catalog table's embedded functions live in the table's own schema.**
  `Worker::set_catalog` registered every table's scan and write functions in
  `main`, whatever schema the table was in, so a `table_only` scan in another
  schema was listed in `main` (a hidden scan named `range` then shadowed
  DuckDB's `range()` in `main`'s macros).
- The example worker serves the `contract` schema for the vgi extension's
  cross-SDK conformance files.

## 0.51.0 (2026-11-12)

- **Protocol 3.3.0** (`VGI_PROTOCOL_VERSION`). Requires the matching VGI
  extension (peers must agree on major + minor); upgrade workers and the
  extension together.
- `FunctionInfo.visibility` (`vgi_protocol::protocol::enums::function_visibility`:
  `"public"` / `"table_only"`; null means public) and `FunctionInfo::is_table_only`.
- Every function a catalog table names (scan, time-travel and branch scans,
  INSERT / UPDATE / DELETE) is always listed, so DuckDB has its capabilities.
  **User-visible:** write functions, and table scans embedded in the table
  (`CatTable::with_function` / `scan_function_impl`), disappear from
  `duckdb_functions()` and are no longer callable directly from SQL, unless the
  table sets the new `CatTable::expose_scan` (`CatTable::with_expose_scan`). A
  scan the author also registers with `Worker::register_table` stays public.
  Write functions are always `table_only`.
- `Worker::hide_function` / `Dispatcher::hide_function` now list the function
  as `table_only` instead of omitting it; for table functions they are no
  longer needed. New `CatalogModel::table_only_function_names`.
- **Protocol 3.3 write-path wire changes** (still 3.3.0; regenerated from
  vgi-python, which also refreshes the round-1 hand-edited generated files and
  their content / `VGI_CATALOG_CONTRACT_HASH` hashes):
  - `catalog_table_scan_function_get` is removed; `catalog_table_scan_branches_get`
    is the only scan-discovery RPC (a single-source table answers one branch,
    as before). The worker-side handler is gone; tables declare their scans as
    before. **vgi-client:** the legacy fallback is deleted —
    `table_scan_branches` no longer retries `catalog_table_scan_function_get`
    on `MethodNotImplementedError` (that is now a protocol error, returned as
    is), `ScanBranchesResolution` keeps only `BranchesRpc` (the
    `LegacyFallbackAfterProbe` / `LegacyCached` variants are removed), and
    `table_scan_function` returns only an inlined scan, erroring for a table
    that does not inline one.
  - `catalog_table_create` answers `created: bool` (new
    `vgi_protocol::protocol::dtos::CatalogTableCreateResult`): true when the
    call created the table, false when `IF NOT EXISTS` found an existing one,
    so `CREATE TABLE IF NOT EXISTS ... AS SELECT` no longer appends into it. The
    stored (DDL) catalog reports it.
  - The `write_options` batch carries only `result_mode`. `WriteOptions::on_conflict`
    and `on_conflict_columns` are **deprecated** and always `"throw"` / empty:
    DuckDB rewrites every `INSERT ... ON CONFLICT` / `INSERT OR REPLACE` into
    `MERGE` before any catalog hook runs, which the extension runs as one write
    call per MERGE action (atomic only on a transactional attach; it needs a
    row id).
- New `vgi::column_expressions::default_metadata` and `DEFAULT`
  (`vgi.column.default.v1`): a canonical column default (a constant
  `SqlExpression`) for a catalog table's columns; a malformed one is rejected.
- Example worker: the `txfix` fixture follows the 3.3 contract — `items.qty`
  is `BIGINT DEFAULT 0`; `items_insert` / `items_update` / `items_delete`
  (`table_only`, all with a finalize phase, up to `changes`) with staging under
  the transaction id on a transactional attach (read-your-writes, rollback and
  refused commits discard); a new `writes()` log (one row per write call:
  `seq, op, tx_id, rows, finalized`), cleared by `reset()`.
  `return_secret_value` declares its `vgi_example` secret statically
  (`required_secrets`), so `vgi_secret_types().required_by` lists it.
