# Catalog metadata and publication (wire 3.0)

SDK 0.48.0 uses `vgi.v3` / `3.0.0`. Upgrade clients and workers
together; published `vgi.v2` peers use a separate route and are incompatible.

`catalog_contents` returns six nullable Arrow IPC streams: `schemas`, `tables`, `views`, `functions`, `macros`, and `indexes`. A stream can contain multiple rows and record batches spanning multiple schemas. Empty categories are null. Functions and macros retain their subtype fields. Per-schema listing RPCs keep their existing item shapes, except `SchemaInfo` no longer carries attachment bytes; real attach request/result and function state remain intact.

The three binary `FunctionInfo` fields `arguments`, `output_schema`, and `parameter_default_values` use `dictionary<int32, binary>`. Author-facing byte values stay unchanged. Equal content shares a dictionary entry; null and empty byte values remain distinct, and readers accept dictionary replacements between batches.


Publication is opt-in. Configure `SnapshotIdentity` with a catalog namespace, a stable source revision, the reported catalog version, and optionally a shared visibility scope. Omitted visibility scope isolates each authenticated attachment. A shared scope is an author declaration that every included caller sees exactly the same metadata. The revision must change when metadata, configuration, or the relevant authorization view changes; do not derive it by serializing the catalog on every request.

The framework opens/authenticates the attachment before consulting publication state. A cold publication collects and validates the catalog, serializes the RPC envelope once, uploads it, fetches it back through the ordinary external-result safety policy, validates its hash/envelopes/all metadata, and conditionally commits its manifest. Warm requests reuse the small descriptor; matching ETags return an inline `not_modified` response. Cold recovery discovers and validates a durable manifest once without rebuilding the catalog. Concurrent requests for the same identity share one publication operation; unrelated identities proceed independently.

`CatalogPublicationRepository` provides storage, discovery, conditional commit/invalidation, location lifetime guarantees, and optional persisted URL renewal. The in-memory repository only survives while its instance survives. `S3CatalogPublicationRepository` provides durable S3/R2 manifests and immutable objects through an injected SDK client: bounded GET, conditional PUT (`If-None-Match`/`If-Match`), and presigned GET. The adapter must propagate authentication/network errors rather than treating them as missing objects or conflicts. Configure bucket retention beyond issued URL lifetimes. Invalidating discovery does not delete objects that existing clients may still fetch.

Defaults reject publication failures, limit raw and stored payloads to 128 MiB each, retain at most 256 validated descriptors, and stop serving a signed URL 60 seconds before expiry. Ordinary-response fallback is an explicit policy option. URL renewal may change only URL and URL expiry for the same immutable manifest. The framework validates a changed location before serving it. Treat manifests and signed URLs as trusted, sensitive server configuration; never accept them directly from RPC callers or log URL credentials.

The manager exposes startup warming, trusted manifest import, and conditional invalidation. Keep identity/revision callbacks cheap and independent of collection serialization. Externalized responses use the existing vgi-rpc result-pointer contract; there is no schema-level publication RPC.

Rust: enable the default `catalog-publication` feature. Install an `Arc<CatalogSnapshotPolicy>` in `CatalogModel.snapshot_policy`, or call `StoredCatalog::snapshot_policy`. Use `policy.warm`, `policy.import`, and `policy.invalidate`. A stored catalog checks its durable session version before consulting the policy.
