# Integration suite in CI

`.github/workflows/integration.yml` runs the **canonical `Query-farm/vgi`
integration sqllogictest suite** against the Rust example worker on every push /
PR, across all three public transports on Linux.

## How it works

The workflow always tests against `Query-farm/vgi` **main**. A Linux producer
job checks out main, resolves it to a full commit SHA once, and publishes that
SHA as the `vgi_sha` job output — from there on nothing references the branch
name. It builds `vgi.duckdb_extension` from that revision, records its SHA-256
and source revision, and uploads it as a one-day workflow artifact. Every
integration and coverage lane checks out the `.test` corpus at the *same*
resolved SHA and downloads and verifies that same artifact.

Resolving once is what makes tracking a moving branch safe. One commit stays
the authority for both the extension and the corpus within a run, so the tests
cannot silently move ahead of the client implementation — which happened when
the workflow tracked main while loading an older community release — and a push
to VGI mid-run cannot hand the producer and the lanes different commits.

The suite still drives a prebuilt standalone `haybarn-unittest`. Its Haybarn
release is pinned to the engine revision the VGI source expects, so the local
extension's ABI/platform metadata matches the runner. VGI loads by
absolute artifact path; `httpfs`/`json`/`parquet`/`spatial` remain signed core
extensions. Because recursive Actions checkouts are shallow and carry no
submodule tags, the producer also sets `OVERRIDE_GIT_DESCRIBE=v1.5.5` and the
pinned `HAYBARN_GIT_DESCRIBE` explicitly; otherwise CMake stamps the extension
with its `v0.0.1` fallback and the strict warm-load check correctly rejects it.

- [`run-integration.sh`](run-integration.sh) — the driver: stages the suite,
  boots the worker(s), and runs `haybarn-unittest` for one transport lane.
- [`preprocess-require.awk`](preprocess-require.awk) — rewrites `require vgi`
  into an absolute load of the verified local artifact, rewrites other
  `require <ext>` gates into core `INSTALL`+`LOAD`, and injects `LOAD httpfs`
  on the http lane.
- [`wrappers/`](wrappers) — the single `vgi-example-worker` binary is routed
  into each catalog (`versioned`, `versioned_tables`, `attach_options`,
  `bad_protocol`) by a wrapper that sets `VGI_WORKER_CATALOG_NAME` and execs it.

## Matrix

| OS | stdio (subprocess) | launch (AF_UNIX) | http |
|----|:------------------:|:----------------:|:----:|
| Linux | ✅ | ✅ | ✅ |

The artifact is a Linux-amd64 native library, so every consumer job runs on
Linux. Building additional native artifacts would require one producer per OS,
which would violate this workflow's build-once invariant. Platform packaging
remains covered by VGI's extension-distribution pipeline; this workflow owns
Rust worker protocol/integration compatibility across the three transports.

## Out of scope / known standalone-runner differences

Dropped at staging (covered by the locally-built `unittest` in the `vgi` repo):

- `writable/` + `simple_writable/` — the write path (deferred read-only port).
- `nested_type_combinations.test` — segfaults the standalone runner.
- `expression_filter.test` — its `EXPLAIN` assertion renders the spatial
  predicate's WKT differently under the prebuilt DuckDB/spatial build.
- http lane only: `projection_pushdown_repro.test`.

## Executed-case floor

`haybarn-unittest` exits 0 whether one test skipped or every test did — a failed
`require` / `require-env` is a **skip**, not an error. So "All tests passed" on
its own is not evidence anything ran: a dead shared worker, an empty stage, or a
mis-wired env var all read as green while the suite quietly tested nothing.
`run_unittest` accumulates the executed count (staged cases minus skips) across
every invocation, and the run fails if it collapses below a per-lane
`MIN_EXECUTED` (stdio 250, launch 255, http 245 — conservative floors below
the current pinned suite, leaving room for environment-dependent skips).
A collapse in that number is the tell of a suite-wide silent skip; it is a floor,
not an equality, so **do not lower it to make a run pass** — find what stopped
running. An empty stage (`No test cases matched`) fails outright.

Only the floor is ported from vgi-typescript, not its per-reason skip allowlist.
The floor alone catches the whole-suite collapses that are the real risk. The
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

`VGI_REF` is deliberately **not** pinned — it is `main`, and the producer job
resolves it to a SHA per run. Do not replace it with a commit: a pin here is
what let vgi-rust drift out of step with the VGI corpus, and pinning is the
thing this workflow is designed to avoid. Consumers still verify the artifact's
recorded source revision and SHA-256, so provenance is unchanged.

`HAYBARN_RELEASE` *is* pinned, to the runner built from the Haybarn engine
revision the extension's ABI/platform metadata must match. Bump it deliberately
when that ABI advances. If a VGI main commit requires a newer engine, the warm
load fails loudly rather than silently mispairing.
