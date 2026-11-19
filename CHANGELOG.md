# Changelog

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
