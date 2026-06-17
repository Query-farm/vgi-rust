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

## Notes / current limitations

- The storage server's idempotency cache is in-memory — sufficient for a single
  instance; a durable/shared cache is needed before running multiple storage
  primaries.
- For a private (non-public) worker, drop `[http_service]` from `worker.fly.toml`
  and reach workers over `.internal` instead.
