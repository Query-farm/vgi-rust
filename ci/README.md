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

## Executed-case floor

`haybarn-unittest` exits 0 whether one test skipped or every test did — a failed
`require` / `require-env` is a **skip**, not an error. So "All tests passed" on
its own is not evidence anything ran: a dead shared worker, an empty stage, or a
mis-wired env var all read as green while the suite quietly tested nothing.
`run_unittest` accumulates the executed count (staged cases minus skips) across
every invocation, and the run fails if it collapses below a per-lane
`MIN_EXECUTED` (stdio 250, launch 255, http 245, Windows-http 240 — floors ~15
below the measured 269 / 270 / 262 / 256, leaving room for upstream growth).
A collapse in that number is the tell of a suite-wide silent skip; it is a floor,
not an equality, so **do not lower it to make a run pass** — find what stopped
running. An empty stage (`No test cases matched`) fails outright.

Only the floor is ported from vgi-typescript, not its per-reason skip allowlist:
this port's three-OS matrix would need per-OS allowlists to maintain, and the
floor alone catches the whole-suite collapses that are the real risk. The
existing fatal-signal scan (a `fatal error condition` block a fork()ed child
prints against the parent's counters, invisible to the exit code) stays.

## Worker coverage

The `coverage` job (Linux only) measures **what the integration suite actually
exercises in the worker** — untested code is a gap in the suite. It builds the
worker with `-Cinstrument-coverage` + the `coverage` feature, runs the suite,
merges the per-worker `.profraw` files, and reports `vgi`-SDK coverage
(`ci/coverage-report.sh`); the `lcov` + text report upload as the
`worker-coverage` artifact, and a digest lands in the job summary.

**Two lanes, merged — for accuracy.** The job runs *both* the stdio (subprocess)
and launch lanes and merges their profiles, because either lane alone is
misleading:

- The pooled launcher / long-lived http workers are killed at teardown without a
  clean exit, so the LLVM `atexit` profile writer never runs. The `coverage`
  feature (`vgi-example-worker/src/coverage.rs`) flushes periodically via a
  background thread, but counters for code that runs *once, early* — notably
  bind-time work like overload resolution — can still be lost. On the launch
  lane alone, `overload.rs` read **~8%** when the suite in fact covers **~95%**.
- The stdio lane spawns a fresh worker per query that exits cleanly, so its
  `atexit`-written counters are reliable — but it never exercises the
  pooled-worker / launcher code paths.

Running both and merging gives reliable bind-time numbers *and* covers the
launcher/pool paths. `ci/coverage-report.sh` validates each `.profraw` before
merging (a worker killed mid-write can leave a truncated file, and one corrupt
input would otherwise abort the whole merge).

## Version pinning

`integration.yml` pins `VGI_REF` (the `Query-farm/vgi` commit whose suite runs)
and `HAYBARN_RELEASE` (the prebuilt runner). The suite and the community `vgi`
extension are coupled — bump `VGI_REF` deliberately and re-validate against the
then-current community extension.
