# Changelog

## Unreleased

- **Cloudflare Durable Object storage backend** (feature `cloudflare-do`).
  `VGI_WORKER_SHARED_STORAGE=cloudflare-do` with `VGI_CF_DO_URL` and
  `VGI_CF_DO_TOKEN` stores worker state in the
  `vgi-cloudflare-durable-object-storage` service the other SDKs use, so Rust
  workers on several hosts share state with no storage server to run.
  `vgi::storage::CfDoStorage` shards by storage scope and retries with a stable
  `attempt_id`. Selecting `cloudflare-do` in a build without the feature now
  fails at startup instead of falling back to per-machine storage.

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
