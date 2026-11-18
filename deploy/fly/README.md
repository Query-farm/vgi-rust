# Deploying VGI workers on fly.io with shared durable storage

This deploys two fly.io apps so **stateless VGI workers scale horizontally** while
sharing cross-process state (buffering, aggregate, work-queue) through one
durable store:

```
                 ATTACH … (TYPE vgi)  over HTTPS
   DuckDB clients ───────────────────────────────▶  vgi-worker  (N machines, public)
                                                        │  stateless; no local state
                          VGI_STORAGE_URL=              │  HTTP storage client (ureq)
                          http://vgi-storage.internal:8080
                                                        ▼
                                                    vgi-storage  (1 machine, internal)
                                                        │  axum + SqliteStorage
                                                        ▼
                                                    /data/state.db  (fly volume)
```

Why this shape: each fly machine has its own filesystem, so the original
`$TMPDIR` store can't be shared across instances. Routing all state to one
storage service means a **single SQLite writer** (no cross-process contention)
and lets workers come and go freely. See `vgi::storage` for the backend trait;
the worker selects the remote backend with `VGI_WORKER_SHARED_STORAGE=http`.

## 1. Storage service (internal, stateful)

```sh
fly apps create vgi-storage
fly volumes create vgi_storage_data --region iad --size 10 -a vgi-storage
fly secrets set VGI_STORAGE_TOKEN=$(openssl rand -hex 32) -a vgi-storage
fly deploy -c deploy/fly/storage-server.fly.toml
```

- Internal-only (no public ports); workers reach it at `vgi-storage.internal:8080`
  over fly's private 6PN network.
- One machine: it is the sole writer to `state.db`. Do **not** scale it past 1
  without LiteFS or shard routing (below) — multiple writers to separate volumes
  would split state.
- Persisted on the volume; survives restarts. Back it up with
  `fly volumes snapshots`.

## 2. Workers (public, stateless)

```sh
fly apps create vgi-worker
fly secrets set \
  VGI_STORAGE_TOKEN=<same token as vgi-storage> \
  VGI_BEARER_TOKENS="<client-token>=<principal>" \
  -a vgi-worker
fly deploy -c deploy/fly/worker.fly.toml
fly scale count 3 -a vgi-worker
```

- `VGI_STORAGE_TOKEN` must match the storage service's token (worker → storage auth).
- `VGI_BEARER_TOKENS` authenticates DuckDB clients to the worker (VGI HTTP transport).
- Scale `count` freely — workers are stateless. fly load-balances ATTACH traffic
  across machines; their shared state stays consistent via `vgi-storage`.

DuckDB side:
```sql
ATTACH 'https://vgi-worker.fly.dev' AS w (TYPE vgi, BEARER_TOKEN '<client-token>');
```

## 3. Verifying

```sh
fly logs -a vgi-storage          # "listening on 0.0.0.0:8080"
fly ssh console -a vgi-worker -C 'wget -qO- http://vgi-storage.internal:8080/health'  # -> ok
```

## Scaling / HA beyond a single storage instance

The single-writer storage service is the availability bottleneck. Two paths when
you outgrow it (both deferred — the protocol already carries a `shard_key` hook):

- **LiteFS** (fly's distributed SQLite): a primary + read replicas with
  streaming replication; writes routed to the primary via `fly-replay`. Gives HA
  with no app changes. Route storage *reads* to the primary too (or use LiteFS
  consistency tokens) so VGI state is never read stale.
- **Shard routing**: partition by `shard_key` (per-ATTACH), each shard owned by a
  machine with its own volume, requests routed with `fly-replay`. Mirrors
  Cloudflare Durable Objects' `idFromName`; scales **writes** horizontally.

## Alternative: Cloudflare Durable Object storage (no storage app to run)

Instead of running `vgi-storage` yourself, workers can share state through the
Cloudflare Durable Object storage service
([`vgi-cloudflare-durable-object-storage`](https://github.com/Query-farm/vgi-cloudflare-durable-object-storage)),
the same multi-tenant service the Python, Go, Java and TypeScript SDKs use.
Build the worker with the `vgi` crate's `cloudflare-do` feature and set:

```sh
fly secrets set \
  VGI_WORKER_SHARED_STORAGE=cloudflare-do \
  VGI_CF_DO_URL=https://vgi-storage.<account>.workers.dev \
  VGI_CF_DO_TOKEN=<api key from `npm run keys -- mint --tenant <svc-env>`> \
  -a vgi-worker
```

| Variable | Meaning |
|---|---|
| `VGI_WORKER_SHARED_STORAGE=cloudflare-do` | Select the backend. A worker built without the `cloudflare-do` feature refuses to start rather than fall back to per-machine storage. |
| `VGI_CF_DO_URL` | Base URL of the storage Worker (required). |
| `VGI_CF_DO_TOKEN` | The worker's API key, sent as `Authorization: Bearer`; it resolves server-side to a tenant that isolates this worker's data. Required by the multi-tenant deployment. Never logged. |

Which to use:

- **`http`** (`vgi-storage` + `SqliteStorage` on a fly volume): state lives in
  your own infrastructure, on a volume you back up, and stays until cleared.
  You run and scale the single-writer storage app, and workers reach it over
  the fly private network.
- **`cloudflare-do`**: nothing to operate, and storage shards across Durable
  Objects instead of one writer. Every request crosses the public internet to
  Cloudflare, and state is **transient**: a Durable Object deletes itself after
  12 h untouched, or 5 min after its scope is cleared. That suits execution
  state (buffering, aggregates, work queues), but not data that must outlive an
  idle period, such as a stored catalog's tables. A single stored value is
  capped at 1 MiB (a larger write fails with 413).

The Rust client shards by storage scope: each execution or transaction id maps
to its own Durable Object (`shard_key = "exe-" + hex(sha256(scope)[..16])`), so
every machine that serves the execution reaches the same state. Destructive
calls carry an `attempt_id` that is reused across retries, so a retried write
whose response was lost is replayed by the server, not applied twice. See
`vgi::storage::cfdo` for the endpoint mapping.

## Notes / current limitations

- The storage server's idempotency cache is in-memory — sufficient for a single
  instance; a durable/shared cache is needed before running multiple storage
  primaries.
- For a private (non-public) worker, drop `[http_service]` from `worker.fly.toml`
  and reach workers over `.internal` instead.
