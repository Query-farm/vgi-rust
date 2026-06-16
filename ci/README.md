# Integration suite in CI

`.github/workflows/integration.yml` runs the **canonical `Query-farm/vgi`
integration sqllogictest suite** against the Rust example worker on every push /
PR, across three transports and three operating systems.

## How it works (no C++ build from source)

Rather than building the DuckDB `vgi` C++ extension, CI drives a **prebuilt
standalone `haybarn-unittest`** (the DuckDB/Haybarn sqllogictest runner) and
installs the *signed* `vgi` extension from the Haybarn **community** channel,
with `httpfs`/`json`/`parquet`/`spatial` from **core**. The `.test` files come
from a pinned `Query-farm/vgi` checkout.

- [`run-integration.sh`](run-integration.sh) — the driver: stages the suite,
  boots the worker(s), and runs `haybarn-unittest` for one transport lane.
- [`preprocess-require.awk`](preprocess-require.awk) — rewrites each
  `require <ext>` gate into a signed `INSTALL`+`LOAD` (the standalone runner
  links none of these extensions), and injects `LOAD httpfs` on the http lane.
- [`wrappers/`](wrappers) — the single `vgi-example-worker` binary is routed
  into each catalog (`versioned`, `versioned_tables`, `attach_options`,
  `bad_protocol`) by a wrapper that sets `VGI_WORKER_CATALOG_NAME` and execs it.

## Matrix

| OS | stdio (subprocess) | launch (AF_UNIX) | http |
|----|:------------------:|:----------------:|:----:|
| Linux  | — (covered by launch) | ✅ | ✅ |
| macOS  | — (covered by launch) | ✅ | ✅ |
| Windows | — (slow; AF_UNIX is Unix-only) | — | ✅ |

The launcher lane runs the whole suite over the AF_UNIX worker pool, so it
covers the subprocess (stdio) path too. **Windows** runs the http lane only: the
worker's AF_UNIX launcher transport is Unix-only, and the bare stdio lane is slow
(a fresh worker process per query). Over http, Windows exercises the main
`example` catalog only (the secondary-catalog tests self-skip via `require-env`,
since the runner can't exec a shell catalog-wrapper as a Windows `LOCATION`) and
drops the fixtures that read parquet/csv from POSIX `/tmp` paths.

## Out of scope / known prebuilt-binary differences

Dropped at staging (covered by the locally-built `unittest` in the `vgi` repo):

- `writable/` + `simple_writable/` — the write path (deferred read-only port).
- `nested_type_combinations.test` — segfaults the prebuilt standalone runner.
- `expression_filter.test` — its `EXPLAIN` assertion renders the spatial
  predicate's WKT differently under the prebuilt DuckDB/spatial build.
- http lane only: `projection_pushdown_repro.test`, `dynamic_filter.test`.

## Version pinning

`integration.yml` pins `VGI_REF` (the `Query-farm/vgi` commit whose suite runs)
and `HAYBARN_RELEASE` (the prebuilt runner). The suite and the community `vgi`
extension are coupled — bump `VGI_REF` deliberately and re-validate against the
then-current community extension.
