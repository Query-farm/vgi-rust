// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! DDL-capable catalogs whose state lives in the worker's shared storage.
//!
//! A [`CatalogModel`](crate::catalog::CatalogModel) is declarative and
//! read-only: every DDL RPC is refused. A [`StoredCatalog`] is the opposite —
//! an attachable catalog that starts empty (one `main` schema) and accepts
//! `CREATE` / `DROP` of schemas, tables and views, bumping its catalog version
//! on every change. It is the Rust analogue of vgi-python's in-memory
//! `CatalogInterface` catalogs, and what the cross-SDK `catalog_contents`
//! fixtures (`contents_memory` / `contents_reval` / `contents_hash`) are built
//! on.
//!
//! **Private per ATTACH.** Every `catalog_attach` mints a fresh session, so two
//! attaches of the same catalog — even through one warm worker — never see each
//! other's objects. `catalog_detach` drops the session's state.
//!
//! **Cross-process.** The subprocess transport pools workers, so the RPCs of
//! one attach can land on different OS processes; an HTTP deployment can spread
//! them over instances. The state therefore lives in the worker's
//! [`FunctionStorage`] (SQLite by default), not in process memory: each DDL is
//! *appended* to the session's event log and every read replays it. Appends
//! are atomic and ordered by the backend, so concurrent writers never lose an
//! event. Each DDL is validated against the replayed state before it is
//! appended; two writers racing on a conflicting DDL may both append, and the
//! replay then skips the one that no longer applies (it does not bump the
//! version) — last-writer semantics are the backend's append order.
//!
//! **Version.** The catalog version starts at 1 and each applied DDL adds 1, so
//! a client polling `catalog_version` sees every change. A catalog built with
//! [`StoredCatalog::unversioned`] instead always reports 0 ("unknown") — the
//! shape whose cache a client must treat as stale at every transaction start.
//!
//! **`catalog_contents`.** Advertised by default. The snapshot is the replayed
//! state; revalidation follows the same two hooks as a declarative catalog — a
//! [`CatalogContentsProvider`] (whose [`CatalogContentsRequest::catalog_version`]
//! is this catalog's live version, a ready-made cheap etag) and/or the opt-in
//! [`CatalogContentsEtag::ContentHash`].
//!
//! Tables created here are metadata only: they appear in every listing with
//! their columns and constraints, but have no rows to scan.

use std::sync::Arc;

use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use vgi_protocol::generated::request_params as p;
use vgi_rpc::{Bytes, DictString, Request, Result, RpcError};

use crate::catalog;
use crate::catalog_contents::{
    finish, CatalogContentsEtag, CatalogContentsProvider, CatalogContentsRequest,
};
use crate::protocol::dtos::{
    CatalogAttachRequest, CatalogAttachResult, CatalogContentsResponse,
    CatalogTransactionBeginResult, CatalogVersionResult, ItemsResult, SchemaContents, SchemaInfo,
    TableCreateRequest, TableInfo, ViewInfo,
};
use crate::storage::FunctionStorage;
use crate::wire;

/// The schema every session starts with (and the attach's default schema).
const DEFAULT_SCHEMA: &str = catalog::MAIN_SCHEMA;

/// A DDL-capable catalog, private per ATTACH, kept in the worker's shared
/// storage. Register it with [`crate::Worker::register_stored_catalog`].
///
/// ```
/// use vgi::stored_catalog::StoredCatalog;
///
/// let mut worker = vgi::Worker::new();
/// worker.register_stored_catalog(StoredCatalog::new("scratch").comment("A scratch catalog"));
/// ```
#[derive(Clone)]
pub struct StoredCatalog {
    name: String,
    comment: Option<String>,
    versioned: bool,
    supports_catalog_contents: bool,
    contents_provider: Option<Arc<dyn CatalogContentsProvider>>,
    catalog_contents_etag: CatalogContentsEtag,
}

impl std::fmt::Debug for StoredCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredCatalog")
            .field("name", &self.name)
            .field("versioned", &self.versioned)
            .field("supports_catalog_contents", &self.supports_catalog_contents)
            .field("contents_provider", &self.contents_provider.is_some())
            .field("catalog_contents_etag", &self.catalog_contents_etag)
            .finish()
    }
}

impl StoredCatalog {
    /// A versioned catalog named `name` that advertises `catalog_contents`
    /// with no etag.
    pub fn new(name: impl Into<String>) -> Self {
        StoredCatalog {
            name: name.into(),
            comment: None,
            versioned: true,
            supports_catalog_contents: true,
            contents_provider: None,
            catalog_contents_etag: CatalogContentsEtag::None,
        }
    }

    /// The catalog's comment (advertised by `catalog_catalogs` / attach).
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Always report catalog version 0 ("unknown"), from `catalog_attach`,
    /// `catalog_version` and `catalog_contents` alike. The DDL still works; the
    /// client just cannot tell whether anything changed.
    pub fn unversioned(mut self) -> Self {
        self.versioned = false;
        self
    }

    /// Whether `catalog_attach` advertises `supports_catalog_contents` (on by
    /// default). The RPC is served either way.
    pub fn catalog_contents(mut self, enabled: bool) -> Self {
        self.supports_catalog_contents = enabled;
        self
    }

    /// The catalog author's `catalog_contents` hook (e.g. a cheap etag from
    /// [`CatalogContentsRequest::catalog_version`]).
    pub fn contents_provider(mut self, provider: Arc<dyn CatalogContentsProvider>) -> Self {
        self.contents_provider = Some(provider);
        self
    }

    /// The opt-in framework etag (see [`CatalogContentsEtag`]).
    pub fn catalog_contents_etag(mut self, etag: CatalogContentsEtag) -> Self {
        self.catalog_contents_etag = etag;
        self
    }

    /// The name the catalog is attached by.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A `CatalogModel` carrying only what `catalog_catalogs` advertises.
    pub(crate) fn discovery_model(&self) -> catalog::CatalogModel {
        catalog::CatalogModel {
            name: self.name.clone(),
            comment: self.comment.clone(),
            ..Default::default()
        }
    }

    // -----------------------------------------------------------------------
    // Session state
    // -----------------------------------------------------------------------

    fn scope(&self, session: &[u8]) -> Vec<u8> {
        let mut scope = b"vgi.stored_catalog\0".to_vec();
        scope.extend_from_slice(self.name.as_bytes());
        scope.push(0);
        scope.extend_from_slice(session);
        scope
    }

    fn append(&self, store: &dyn FunctionStorage, session: &[u8], event: &Event) -> Result<()> {
        let bytes = serde_json::to_vec(event)
            .map_err(|e| RpcError::runtime_error(format!("{}: encode DDL: {e}", self.name)))?;
        store.append(&self.scope(session), LOG_NS, LOG_KEY, bytes);
        Ok(())
    }

    /// Replay the session's event log. Errors when the session was never
    /// attached here (or was detached / garbage-collected).
    fn load(&self, store: &dyn FunctionStorage, session: &[u8]) -> Result<State> {
        let events = store.scan(&self.scope(session), LOG_NS, LOG_KEY, -1, usize::MAX);
        let mut events = events.into_iter().map(|(_, bytes)| {
            serde_json::from_slice::<Event>(&bytes).map_err(|e| {
                RpcError::runtime_error(format!("{}: corrupt DDL log: {e}", self.name))
            })
        });
        match events.next().transpose()? {
            Some(Event::Attached) => {}
            _ => {
                return Err(RpcError::value_error(format!(
                    "catalog '{}' is not attached (unknown or detached session)",
                    self.name
                )))
            }
        }
        let mut state = State::new();
        for event in events {
            // A DDL that no longer applies lost a race with a conflicting
            // one appended before it: skip it, without a version bump.
            if let Ok(true) = state.apply(&event?) {
                state.version += 1;
            }
        }
        Ok(state)
    }

    /// Validate `event` against the current state and, when it changes
    /// anything, append it.
    fn mutate(&self, store: &dyn FunctionStorage, session: &[u8], event: Event) -> Result<()> {
        let mut state = self.load(store, session)?;
        if state.apply(&event)? {
            self.append(store, session, &event)?;
        }
        Ok(())
    }

    fn reported_version(&self, state: &State) -> i64 {
        if self.versioned {
            state.version
        } else {
            0
        }
    }

    // -----------------------------------------------------------------------
    // RPCs
    // -----------------------------------------------------------------------

    /// `catalog_attach`: start a fresh, empty session. `attach_opaque_data` is
    /// the handle the dispatcher minted for it.
    pub(crate) fn attach(
        &self,
        store: &dyn FunctionStorage,
        session: &[u8],
        attach_opaque_data: Vec<u8>,
        _dto: &CatalogAttachRequest,
    ) -> Result<CatalogAttachResult> {
        store.clear(&self.scope(session));
        self.append(store, session, &Event::Attached)?;
        let state = self.load(store, session)?;
        Ok(CatalogAttachResult {
            attach_opaque_data: Bytes::from(attach_opaque_data),
            supports_transactions: false,
            supports_time_travel: false,
            catalog_version_frozen: false,
            catalog_version: self.reported_version(&state),
            attach_opaque_data_required: true,
            default_schema: DEFAULT_SCHEMA.to_string(),
            settings: Vec::new(),
            secret_types: Vec::new(),
            attach_catalogs: Vec::new(),
            comment: self.comment.clone(),
            tags: Vec::new(),
            supports_column_statistics: false,
            global_functions: Vec::new(),
            global_function_prefix: String::new(),
            resolved_data_version: None,
            resolved_implementation_version: None,
            supports_catalog_contents: self.supports_catalog_contents,
        })
    }

    /// Serve one catalog RPC for a session of this catalog.
    pub(crate) fn handle(
        &self,
        store: &dyn FunctionStorage,
        attach: &[u8],
        session: &[u8],
        req: &Request,
    ) -> Result<Option<RecordBatch>> {
        let items = |items: Vec<Bytes>| Ok(Some(wire::to_result_batch(ItemsResult { items })?));
        match req.method.as_str() {
            "catalog_detach" => {
                store.clear(&self.scope(session));
                Ok(None)
            }
            "catalog_version" => {
                let state = self.load(store, session)?;
                Ok(Some(wire::to_result_batch(CatalogVersionResult {
                    version: self.reported_version(&state),
                })?))
            }
            "catalog_transaction_begin" => Ok(Some(wire::to_result_batch(
                CatalogTransactionBeginResult {
                    transaction_opaque_data: None,
                },
            )?)),
            "catalog_transaction_commit" | "catalog_transaction_rollback" => Ok(None),
            "catalog_schemas" => {
                let state = self.load(store, session)?;
                items(catalog::serialize_items(
                    state
                        .schemas
                        .iter()
                        .map(|s| s.info(attach))
                        .collect::<Vec<_>>(),
                )?)
            }
            "catalog_schema_get" => {
                let params: p::CatalogSchemaGetParams = wire::from_batch(&req.batch)?;
                let state = self.load(store, session)?;
                let found: Vec<SchemaInfo> = state
                    .schema(&params.path)
                    .map(|s| s.info(attach))
                    .into_iter()
                    .collect();
                items(catalog::serialize_items(found)?)
            }
            "catalog_schema_contents_tables" => {
                let params: p::CatalogSchemaContentsTablesParams = wire::from_batch(&req.batch)?;
                let state = self.load(store, session)?;
                items(state.table_items(&params.path)?)
            }
            "catalog_schema_contents_views" => {
                let params: p::CatalogSchemaContentsViewsParams = wire::from_batch(&req.batch)?;
                let state = self.load(store, session)?;
                items(state.view_items(&params.path)?)
            }
            // Functions, macros and indexes: this catalog models none.
            "catalog_schema_contents_functions"
            | "catalog_schema_contents_macros"
            | "catalog_schema_contents_indexes"
            | "catalog_macro_get"
            | "catalog_index_get"
            | "catalog_copy_from_formats" => items(Vec::new()),
            "catalog_table_get" => {
                let params: p::CatalogTableGetParams = wire::from_batch(&req.batch)?;
                let state = self.load(store, session)?;
                let found = state
                    .schema(&params.schema_path)
                    .and_then(|s| s.table(&params.name))
                    .map(|t| t.info(&params.schema_path))
                    .transpose()?;
                items(catalog::serialize_items(found.into_iter().collect())?)
            }
            "catalog_view_get" => {
                let params: p::CatalogViewGetParams = wire::from_batch(&req.batch)?;
                let state = self.load(store, session)?;
                let found: Vec<ViewInfo> = state
                    .schema(&params.schema_path)
                    .and_then(|s| s.view(&params.name))
                    .map(|v| v.info(&params.schema_path))
                    .into_iter()
                    .collect();
                items(catalog::serialize_items(found)?)
            }
            "catalog_table_column_statistics_get" => Ok(Some(wire::result_batch_from_bytes(
                &crate::statistics::serialize_column_statistics(&[])?,
            )?)),
            "catalog_table_scan_function_get" | "catalog_table_scan_branches_get" => {
                Err(RpcError::value_error(format!(
                    "catalog '{}' stores table metadata only; its tables have no rows to scan",
                    self.name
                )))
            }
            "catalog_contents" => {
                let params: p::CatalogContentsParams = wire::from_batch(&req.batch)?;
                let response =
                    self.contents(store, attach, session, params.if_none_match.as_deref())?;
                Ok(Some(wire::to_result_batch(response)?))
            }
            "catalog_schema_create" => {
                let q: p::CatalogSchemaCreateParams = wire::from_batch(&req.batch)?;
                self.mutate(
                    store,
                    session,
                    Event::SchemaCreate {
                        path: q.path,
                        comment: q.comment,
                        tags: q.tags.unwrap_or_default(),
                        on_conflict: OnConflict::parse(&q.on_conflict)?,
                    },
                )?;
                Ok(None)
            }
            "catalog_schema_drop" => {
                let q: p::CatalogSchemaDropParams = wire::from_batch(&req.batch)?;
                self.mutate(
                    store,
                    session,
                    Event::SchemaDrop {
                        path: q.path,
                        ignore_not_found: q.ignore_not_found,
                        cascade: q.cascade,
                    },
                )?;
                Ok(None)
            }
            "catalog_table_create" => {
                let boxed: p::CatalogTableCreateParams = wire::from_batch(&req.batch)?;
                let q: TableCreateRequest =
                    wire::from_batch(&crate::ipc::read_batch(&boxed.request.0)?)?;
                // Reject a malformed schema now rather than on every listing.
                crate::ipc::read_schema(&q.columns.0)?;
                self.mutate(
                    store,
                    session,
                    Event::TableCreate {
                        path: q.schema_path,
                        name: q.name,
                        columns: q.columns.0.to_vec(),
                        not_null: q.not_null_constraints,
                        unique: q.unique_constraints,
                        check: q.check_constraints,
                        primary_key: q.primary_key_constraints,
                        on_conflict: OnConflict::parse(&q.on_conflict)?,
                    },
                )?;
                Ok(None)
            }
            "catalog_table_drop" => {
                let q: p::CatalogTableDropParams = wire::from_batch(&req.batch)?;
                self.mutate(
                    store,
                    session,
                    Event::TableDrop {
                        path: q.schema_path,
                        name: q.name,
                        ignore_not_found: q.ignore_not_found,
                    },
                )?;
                Ok(None)
            }
            "catalog_view_create" => {
                let q: p::CatalogViewCreateParams = wire::from_batch(&req.batch)?;
                self.mutate(
                    store,
                    session,
                    Event::ViewCreate {
                        path: q.schema_path,
                        name: q.name,
                        definition: q.definition,
                        on_conflict: OnConflict::parse(&q.on_conflict)?,
                    },
                )?;
                Ok(None)
            }
            "catalog_view_drop" => {
                let q: p::CatalogViewDropParams = wire::from_batch(&req.batch)?;
                self.mutate(
                    store,
                    session,
                    Event::ViewDrop {
                        path: q.schema_path,
                        name: q.name,
                        ignore_not_found: q.ignore_not_found,
                    },
                )?;
                Ok(None)
            }
            other => Err(RpcError::value_error(format!(
                "catalog '{}' does not support {other}",
                self.name
            ))),
        }
    }

    /// `catalog_contents` for a session: the replayed state, shaped by the
    /// provider / content-hash etag and the wire rules.
    pub(crate) fn contents(
        &self,
        store: &dyn FunctionStorage,
        attach: &[u8],
        session: &[u8],
        if_none_match: Option<&str>,
    ) -> Result<CatalogContentsResponse> {
        let state = self.load(store, session)?;
        let version = self.reported_version(&state);
        let build = || state.snapshot(attach);
        let request = CatalogContentsRequest {
            attach_opaque_data: attach,
            if_none_match,
            catalog_version: version,
            generation: state.version.max(0) as u64,
            build: &build,
        };
        let result = match &self.contents_provider {
            Some(provider) => provider.catalog_contents(&request)?,
            None => request.default_contents()?,
        };
        finish(result, if_none_match, version, self.catalog_contents_etag)
    }
}

// ---------------------------------------------------------------------------
// Event log + replayed state
// ---------------------------------------------------------------------------

const LOG_NS: &[u8] = b"ddl";
const LOG_KEY: &[u8] = b"log";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum OnConflict {
    Error,
    Ignore,
    Replace,
}

impl OnConflict {
    fn parse(value: &DictString) -> Result<Self> {
        match value.0.to_ascii_lowercase().as_str() {
            "error" | "" => Ok(OnConflict::Error),
            "ignore" => Ok(OnConflict::Ignore),
            "replace" => Ok(OnConflict::Replace),
            other => Err(RpcError::value_error(format!(
                "unknown on_conflict '{other}' (expected error, ignore or replace)"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Event {
    Attached,
    SchemaCreate {
        path: Vec<String>,
        comment: Option<String>,
        tags: Vec<(String, String)>,
        on_conflict: OnConflict,
    },
    SchemaDrop {
        path: Vec<String>,
        ignore_not_found: bool,
        cascade: bool,
    },
    TableCreate {
        path: Vec<String>,
        name: String,
        columns: Vec<u8>,
        not_null: Vec<i32>,
        unique: Vec<Vec<i32>>,
        check: Vec<String>,
        primary_key: Vec<Vec<i32>>,
        on_conflict: OnConflict,
    },
    TableDrop {
        path: Vec<String>,
        name: String,
        ignore_not_found: bool,
    },
    ViewCreate {
        path: Vec<String>,
        name: String,
        definition: String,
        on_conflict: OnConflict,
    },
    ViewDrop {
        path: Vec<String>,
        name: String,
        ignore_not_found: bool,
    },
}

#[derive(Debug, Clone)]
struct StoredTable {
    name: String,
    columns: Vec<u8>,
    not_null: Vec<i32>,
    unique: Vec<Vec<i32>>,
    check: Vec<String>,
    primary_key: Vec<Vec<i32>>,
}

impl StoredTable {
    fn info(&self, path: &[String]) -> Result<TableInfo> {
        let mut t = catalog::CatTable::new(
            &self.name,
            crate::ipc::read_schema(&self.columns)?,
            "",
            Vec::new(),
            None,
            None,
        );
        t.not_null = self.not_null.clone();
        t.unique = self.unique.clone();
        t.check = self.check.clone();
        t.primary_key = self.primary_key.clone();
        catalog::table_info(path, &t, None)
    }
}

#[derive(Debug, Clone)]
struct StoredView {
    name: String,
    definition: String,
}

impl StoredView {
    fn info(&self, path: &[String]) -> ViewInfo {
        ViewInfo {
            comment: None,
            tags: Vec::new(),
            name: self.name.clone(),
            schema_path: path.to_vec(),
            definition: self.definition.clone(),
            column_comments: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct StoredSchema {
    path: Vec<String>,
    comment: Option<String>,
    tags: Vec<(String, String)>,
    tables: Vec<StoredTable>,
    views: Vec<StoredView>,
}

/// DuckDB identifiers are case-insensitive.
fn same(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

fn same_path(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| same(x, y))
}

impl StoredSchema {
    fn empty(path: Vec<String>) -> Self {
        StoredSchema {
            path,
            comment: None,
            tags: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
        }
    }

    fn table(&self, name: &str) -> Option<&StoredTable> {
        self.tables.iter().find(|t| same(&t.name, name))
    }

    fn view(&self, name: &str) -> Option<&StoredView> {
        self.views.iter().find(|v| same(&v.name, name))
    }

    /// The `SchemaInfo` item, with exact per-kind counts: this catalog models
    /// only tables and views, so every other kind is a hard 0.
    fn info(&self, attach: &[u8]) -> SchemaInfo {
        let mut info = catalog::schema_info(&self.path, self.comment.as_deref(), attach);
        info.tags = self.tags.clone();
        info.estimated_object_count = Some(vec![
            ("table".into(), self.tables.len() as i64),
            ("view".into(), self.views.len() as i64),
            ("macro".into(), 0),
            ("index".into(), 0),
            ("scalar_function".into(), 0),
            ("aggregate_function".into(), 0),
            ("table_function".into(), 0),
        ]);
        info
    }
}

#[derive(Debug, Clone)]
struct State {
    version: i64,
    schemas: Vec<StoredSchema>,
}

impl State {
    fn new() -> Self {
        State {
            version: 1,
            schemas: vec![StoredSchema::empty(vec![DEFAULT_SCHEMA.to_string()])],
        }
    }

    fn schema(&self, path: &[String]) -> Option<&StoredSchema> {
        self.schemas.iter().find(|s| same_path(&s.path, path))
    }

    fn schema_mut(&mut self, path: &[String]) -> Result<&mut StoredSchema> {
        self.schemas
            .iter_mut()
            .find(|s| same_path(&s.path, path))
            .ok_or_else(|| RpcError::value_error(format!("Schema '{}' not found", path.join("."))))
    }

    fn table_items(&self, path: &[String]) -> Result<Vec<Bytes>> {
        let infos = match self.schema(path) {
            Some(s) => s
                .tables
                .iter()
                .map(|t| t.info(&s.path))
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        catalog::serialize_items(infos)
    }

    fn view_items(&self, path: &[String]) -> Result<Vec<Bytes>> {
        let infos: Vec<ViewInfo> = self
            .schema(path)
            .map(|s| s.views.iter().map(|v| v.info(&s.path)).collect())
            .unwrap_or_default();
        catalog::serialize_items(infos)
    }

    /// Every schema and its contents, each item what the per-schema RPC
    /// answers.
    fn snapshot(&self, attach: &[u8]) -> Result<Vec<SchemaContents>> {
        self.schemas
            .iter()
            .map(|s| {
                let schema = catalog::serialize_items(vec![s.info(attach)])?
                    .pop()
                    .ok_or_else(|| RpcError::runtime_error("SchemaInfo did not serialize"))?;
                Ok(SchemaContents {
                    path: s.path.clone(),
                    schema,
                    tables: self.table_items(&s.path)?,
                    views: self.view_items(&s.path)?,
                    scalar_functions: Vec::new(),
                    aggregate_functions: Vec::new(),
                    table_functions: Vec::new(),
                    scalar_macros: Vec::new(),
                    table_macros: Vec::new(),
                    indexes: Vec::new(),
                })
            })
            .collect()
    }

    /// Apply one DDL. `Ok(true)`: the catalog changed (bump the version);
    /// `Ok(false)`: a no-op (`IF NOT EXISTS` / `IF EXISTS` on nothing).
    fn apply(&mut self, event: &Event) -> Result<bool> {
        match event {
            Event::Attached => Ok(false),
            Event::SchemaCreate {
                path,
                comment,
                tags,
                on_conflict,
            } => {
                if path.is_empty() {
                    return Err(RpcError::value_error("schema path must not be empty"));
                }
                if path.len() > 1 && self.schema(&path[..path.len() - 1]).is_none() {
                    return Err(RpcError::value_error(format!(
                        "Schema '{}' not found",
                        path[..path.len() - 1].join(".")
                    )));
                }
                let mut fresh = StoredSchema::empty(path.clone());
                fresh.comment = comment.clone();
                fresh.tags = tags.clone();
                if let Some(i) = self.schemas.iter().position(|s| same_path(&s.path, path)) {
                    return match on_conflict {
                        OnConflict::Ignore => Ok(false),
                        OnConflict::Replace => {
                            self.schemas[i] = fresh;
                            Ok(true)
                        }
                        OnConflict::Error => Err(RpcError::value_error(format!(
                            "Schema with name '{}' already exists",
                            path.join(".")
                        ))),
                    };
                }
                self.schemas.push(fresh);
                Ok(true)
            }
            Event::SchemaDrop {
                path,
                ignore_not_found,
                cascade,
            } => {
                let Some(i) = self.schemas.iter().position(|s| same_path(&s.path, path)) else {
                    return if *ignore_not_found {
                        Ok(false)
                    } else {
                        Err(RpcError::value_error(format!(
                            "Schema '{}' not found",
                            path.join(".")
                        )))
                    };
                };
                if same_path(path, &[DEFAULT_SCHEMA.to_string()]) {
                    return Err(RpcError::value_error(format!(
                        "cannot drop the default schema '{DEFAULT_SCHEMA}'"
                    )));
                }
                let is_child = |s: &StoredSchema| {
                    s.path.len() > path.len() && same_path(&s.path[..path.len()], path)
                };
                let s = &self.schemas[i];
                let occupied = !s.tables.is_empty()
                    || !s.views.is_empty()
                    || self.schemas.iter().any(is_child);
                if occupied && !cascade {
                    return Err(RpcError::value_error(format!(
                        "Schema '{}' is not empty; use CASCADE",
                        path.join(".")
                    )));
                }
                self.schemas
                    .retain(|s| !same_path(&s.path, path) && !is_child(s));
                Ok(true)
            }
            Event::TableCreate {
                path,
                name,
                columns,
                not_null,
                unique,
                check,
                primary_key,
                on_conflict,
            } => {
                let table = StoredTable {
                    name: name.clone(),
                    columns: columns.clone(),
                    not_null: not_null.clone(),
                    unique: unique.clone(),
                    check: check.clone(),
                    primary_key: primary_key.clone(),
                };
                let schema = self.schema_mut(path)?;
                if schema.view(name).is_some() {
                    return Err(RpcError::value_error(format!(
                        "A view named '{name}' already exists"
                    )));
                }
                match schema.tables.iter().position(|t| same(&t.name, name)) {
                    Some(i) => match on_conflict {
                        OnConflict::Ignore => Ok(false),
                        OnConflict::Replace => {
                            schema.tables[i] = table;
                            Ok(true)
                        }
                        OnConflict::Error => Err(RpcError::value_error(format!(
                            "Table with name '{name}' already exists"
                        ))),
                    },
                    None => {
                        schema.tables.push(table);
                        Ok(true)
                    }
                }
            }
            Event::TableDrop {
                path,
                name,
                ignore_not_found,
            } => drop_named(
                self,
                path,
                name,
                *ignore_not_found,
                "Table",
                |s| &mut s.tables,
                |t| &t.name,
            ),
            Event::ViewCreate {
                path,
                name,
                definition,
                on_conflict,
            } => {
                let view = StoredView {
                    name: name.clone(),
                    definition: definition.clone(),
                };
                let schema = self.schema_mut(path)?;
                if schema.table(name).is_some() {
                    return Err(RpcError::value_error(format!(
                        "A table named '{name}' already exists"
                    )));
                }
                match schema.views.iter().position(|v| same(&v.name, name)) {
                    Some(i) => match on_conflict {
                        OnConflict::Ignore => Ok(false),
                        OnConflict::Replace => {
                            schema.views[i] = view;
                            Ok(true)
                        }
                        OnConflict::Error => Err(RpcError::value_error(format!(
                            "View with name '{name}' already exists"
                        ))),
                    },
                    None => {
                        schema.views.push(view);
                        Ok(true)
                    }
                }
            }
            Event::ViewDrop {
                path,
                name,
                ignore_not_found,
            } => drop_named(
                self,
                path,
                name,
                *ignore_not_found,
                "View",
                |s| &mut s.views,
                |v| &v.name,
            ),
        }
    }
}

/// Remove the object `name` of one kind from schema `path`.
fn drop_named<T>(
    state: &mut State,
    path: &[String],
    name: &str,
    ignore_not_found: bool,
    kind: &str,
    list: impl Fn(&mut StoredSchema) -> &mut Vec<T>,
    name_of: impl Fn(&T) -> &String,
) -> Result<bool> {
    let missing = || {
        if ignore_not_found {
            Ok(false)
        } else {
            Err(RpcError::value_error(format!(
                "{kind} '{}.{name}' not found",
                path.join(".")
            )))
        }
    };
    let Some(schema) = state.schemas.iter_mut().find(|s| same_path(&s.path, path)) else {
        return missing();
    };
    let items = list(schema);
    match items.iter().position(|item| same(name_of(item), name)) {
        Some(i) => {
            items.remove(i);
            Ok(true)
        }
        None => missing(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_contents::{catalog_contents_digest, CatalogContentsResult};
    use crate::storage::MemoryStorage;
    use arrow_schema::{DataType, Field, Schema};

    fn request(method: &str, batch: RecordBatch) -> Request {
        Request {
            method: method.to_string(),
            protocol: String::new(),
            request_id: String::new(),
            batch,
            metadata: Arc::new(Default::default()),
        }
    }

    fn path(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    struct Session {
        cat: StoredCatalog,
        store: MemoryStorage,
        session: Vec<u8>,
    }

    impl Session {
        fn new(cat: StoredCatalog) -> Self {
            let store = MemoryStorage::new();
            let session = b"s1".to_vec();
            cat.attach(&store, &session, b"attach".to_vec(), &attach_request())
                .unwrap();
            Session {
                cat,
                store,
                session,
            }
        }

        fn call(&self, method: &str, batch: RecordBatch) -> Result<Option<RecordBatch>> {
            self.cat.handle(
                &self.store,
                b"attach",
                &self.session,
                &request(method, batch),
            )
        }

        fn version(&self) -> i64 {
            let batch = wire::to_batch(p::CatalogVersionParams {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                transaction_opaque_data: None,
            })
            .unwrap();
            let r: CatalogVersionResult = wire::from_batch(&inner(
                self.call("catalog_version", batch).unwrap().unwrap(),
            ))
            .unwrap();
            r.version
        }

        fn create_view(&self, name: &str, on_conflict: &str) -> Result<Option<RecordBatch>> {
            let batch = wire::to_batch(p::CatalogViewCreateParams {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                schema_path: path(&["main"]),
                name: name.to_string(),
                definition: "SELECT 1 AS x".to_string(),
                on_conflict: DictString(on_conflict.to_string()),
                transaction_opaque_data: None,
            })
            .unwrap();
            self.call("catalog_view_create", batch)
        }

        fn drop_view(&self, name: &str, ignore_not_found: bool) -> Result<Option<RecordBatch>> {
            let batch = wire::to_batch(p::CatalogViewDropParams {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                schema_path: path(&["main"]),
                name: name.to_string(),
                ignore_not_found,
                cascade: false,
                transaction_opaque_data: None,
            })
            .unwrap();
            self.call("catalog_view_drop", batch)
        }

        fn create_table(&self, schema: &[&str], name: &str) -> Result<Option<RecordBatch>> {
            let columns = Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]);
            let inner = wire::to_batch(TableCreateRequest {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                schema_path: path(schema),
                name: name.to_string(),
                columns: Bytes::from(crate::ipc::write_schema(&columns).unwrap()),
                on_conflict: DictString("error".to_string()),
                not_null_constraints: vec![0],
                unique_constraints: Vec::new(),
                check_constraints: Vec::new(),
                primary_key_constraints: Vec::new(),
                foreign_key_constraints: Vec::new(),
                transaction_opaque_data: None,
            })
            .unwrap();
            let batch = wire::to_batch(p::CatalogTableCreateParams {
                request: Bytes::from(crate::ipc::write_batch(&inner).unwrap()),
            })
            .unwrap();
            self.call("catalog_table_create", batch)
        }

        fn create_schema(&self, schema: &[&str]) -> Result<Option<RecordBatch>> {
            let batch = wire::to_batch(p::CatalogSchemaCreateParams {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                path: path(schema),
                on_conflict: DictString("error".to_string()),
                comment: Some("made here".to_string()),
                tags: None,
                transaction_opaque_data: None,
            })
            .unwrap();
            self.call("catalog_schema_create", batch)
        }

        fn list<T: vgi_rpc::VgiArrow>(&self, method: &str, schema: &[&str]) -> Vec<T> {
            let batch = wire::to_batch(p::CatalogSchemaContentsViewsParams {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                path: path(schema),
                transaction_opaque_data: None,
            })
            .unwrap();
            let r: ItemsResult =
                wire::from_batch(&inner(self.call(method, batch).unwrap().unwrap())).unwrap();
            r.items
                .iter()
                .map(|b| wire::from_batch(&crate::ipc::read_batch(&b.0).unwrap()).unwrap())
                .collect()
        }

        fn contents(&self, if_none_match: Option<&str>) -> CatalogContentsResponse {
            self.cat
                .contents(&self.store, b"attach", &self.session, if_none_match)
                .unwrap()
        }
    }

    fn attach_request() -> CatalogAttachRequest {
        CatalogAttachRequest {
            name: "mem".to_string(),
            options: None,
            data_version_spec: None,
            implementation_version: None,
            client_capabilities: None,
        }
    }

    fn inner(result: RecordBatch) -> RecordBatch {
        let envelope = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        crate::ipc::read_batch(envelope.value(0)).unwrap()
    }

    #[test]
    fn starts_empty_and_versions_every_change() {
        let s = Session::new(StoredCatalog::new("mem"));
        assert_eq!(s.version(), 1);
        let schemas: Vec<SchemaInfo> = s.list("catalog_schemas", &[]);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].path, path(&["main"]));

        s.create_view("v1", "error").unwrap();
        assert_eq!(s.version(), 2);
        s.create_table(&["main"], "t1").unwrap();
        assert_eq!(s.version(), 3);
        let views: Vec<ViewInfo> = s.list("catalog_schema_contents_views", &["main"]);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].name, "v1");
        let tables: Vec<TableInfo> = s.list("catalog_schema_contents_tables", &["main"]);
        assert_eq!(tables[0].name, "t1");
        assert_eq!(tables[0].not_null_constraints, vec![0]);
        let cols = crate::ipc::read_schema(&tables[0].columns.0).unwrap();
        assert_eq!(cols.field(0).data_type(), &DataType::Int32);

        s.drop_view("v1", false).unwrap();
        assert_eq!(s.version(), 4);
        let views: Vec<ViewInfo> = s.list("catalog_schema_contents_views", &["main"]);
        assert!(views.is_empty());
    }

    #[test]
    fn conflicts_and_missing_objects() {
        let s = Session::new(StoredCatalog::new("mem"));
        s.create_view("v", "error").unwrap();
        let err = s.create_view("V", "error").unwrap_err().to_string();
        assert!(err.contains("already exists"), "{err}");
        // IF NOT EXISTS is a no-op: no version bump.
        s.create_view("v", "ignore").unwrap();
        assert_eq!(s.version(), 2);
        // OR REPLACE changes the catalog.
        s.create_view("v", "replace").unwrap();
        assert_eq!(s.version(), 3);
        // A table cannot share a view's name.
        assert!(s.create_table(&["main"], "v").is_err());
        // DROP of nothing.
        assert!(s.drop_view("nope", false).is_err());
        s.drop_view("nope", true).unwrap();
        assert_eq!(s.version(), 3);
        // A table in a missing schema.
        assert!(s.create_table(&["nowhere"], "t").is_err());
    }

    #[test]
    fn schemas_nest_and_drop_with_cascade() {
        let s = Session::new(StoredCatalog::new("mem"));
        s.create_schema(&["s2"]).unwrap();
        assert!(s.create_schema(&["s2"]).is_err());
        assert!(s.create_schema(&["missing", "child"]).is_err());
        s.create_schema(&["s2", "inner"]).unwrap();
        s.create_table(&["s2"], "t").unwrap();
        let drop = |cascade| {
            let batch = wire::to_batch(p::CatalogSchemaDropParams {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                path: path(&["s2"]),
                ignore_not_found: false,
                cascade,
                transaction_opaque_data: None,
            })
            .unwrap();
            s.call("catalog_schema_drop", batch)
        };
        assert!(drop(false).unwrap_err().to_string().contains("CASCADE"));
        drop(true).unwrap();
        let schemas: Vec<SchemaInfo> = s.list("catalog_schemas", &[]);
        assert_eq!(schemas.len(), 1, "the child went with its parent");
    }

    #[test]
    fn attaches_are_private_and_detach_forgets() {
        let cat = StoredCatalog::new("mem");
        let store = MemoryStorage::new();
        cat.attach(&store, b"a", b"A".to_vec(), &attach_request())
            .unwrap();
        cat.attach(&store, b"b", b"B".to_vec(), &attach_request())
            .unwrap();
        let a = Session {
            cat: cat.clone(),
            store,
            session: b"a".to_vec(),
        };
        a.create_view("only_in_a", "error").unwrap();
        let b = Session {
            session: b"b".to_vec(),
            ..a
        };
        let views: Vec<ViewInfo> = b.list("catalog_schema_contents_views", &["main"]);
        assert!(views.is_empty());
        assert_eq!(b.version(), 1);

        let detach = wire::to_batch(p::CatalogDetachParams {
            attach_opaque_data: Bytes::from(b"B".to_vec()),
        })
        .unwrap();
        b.call("catalog_detach", detach).unwrap();
        let err = b.contents_err();
        assert!(err.contains("not attached"), "{err}");
    }

    impl Session {
        fn contents_err(&self) -> String {
            self.cat
                .contents(&self.store, b"attach", &self.session, None)
                .unwrap_err()
                .to_string()
        }
    }

    /// The event log is the state: a second "process" (a fresh `StoredCatalog`
    /// value over the same storage) sees every change.
    #[test]
    fn state_is_shared_through_storage() {
        let s = Session::new(StoredCatalog::new("mem"));
        s.create_view("v", "error").unwrap();
        let other = StoredCatalog::new("mem");
        let state = other.load(&s.store, &s.session).unwrap();
        assert_eq!(state.version, 2);
        assert_eq!(state.schema(&path(&["main"])).unwrap().views.len(), 1);
    }

    /// A conflicting DDL appended by a racing writer is skipped on replay,
    /// without a version bump.
    #[test]
    fn replay_skips_a_ddl_that_lost_a_race() {
        let s = Session::new(StoredCatalog::new("mem"));
        let ev = Event::ViewCreate {
            path: path(&["main"]),
            name: "v".to_string(),
            definition: "SELECT 1".to_string(),
            on_conflict: OnConflict::Error,
        };
        s.cat.append(&s.store, &s.session, &ev).unwrap();
        s.cat.append(&s.store, &s.session, &ev).unwrap();
        assert_eq!(s.version(), 2);
    }

    #[test]
    fn unversioned_reports_zero_everywhere() {
        let cat = StoredCatalog::new("mem").unversioned();
        let store = MemoryStorage::new();
        let attached = cat
            .attach(&store, b"s", b"A".to_vec(), &attach_request())
            .unwrap();
        assert_eq!(attached.catalog_version, 0);
        let s = Session {
            cat,
            store,
            session: b"s".to_vec(),
        };
        s.create_view("v", "error").unwrap();
        assert_eq!(s.version(), 0);
        let c = s.contents(None);
        assert_eq!(c.catalog_version, 0);
        assert_eq!(c.etag, None);
        assert_eq!(c.schemas.len(), 1);
        assert_eq!(c.schemas[0].views.len(), 1);
    }

    struct Generation;
    impl CatalogContentsProvider for Generation {
        fn catalog_contents(
            &self,
            req: &CatalogContentsRequest<'_>,
        ) -> Result<CatalogContentsResult> {
            let etag = format!("gen-{}", req.catalog_version());
            if req.if_none_match() == Some(etag.as_str()) {
                return Ok(CatalogContentsResult::not_modified(etag));
            }
            Ok(CatalogContentsResult::full(req.build()?, Some(etag)))
        }
    }

    #[test]
    fn provider_etag_tracks_the_version() {
        let s = Session::new(StoredCatalog::new("mem").contents_provider(Arc::new(Generation)));
        let first = s.contents(None);
        assert_eq!(first.etag.as_deref(), Some("gen-1"));
        assert_eq!(first.catalog_version, 1);
        let again = s.contents(Some("gen-1"));
        assert!(again.not_modified && again.schemas.is_empty());
        s.create_view("v", "error").unwrap();
        let changed = s.contents(Some("gen-1"));
        assert!(!changed.not_modified);
        assert_eq!(changed.etag.as_deref(), Some("gen-2"));
        assert_eq!(changed.catalog_version, 2);
    }

    #[test]
    fn content_hash_etag_changes_with_the_contents() {
        let s = Session::new(
            StoredCatalog::new("mem").catalog_contents_etag(CatalogContentsEtag::ContentHash),
        );
        let first = s.contents(None);
        let etag = first.etag.clone().unwrap();
        assert_eq!(etag, catalog_contents_digest(&first.schemas));
        assert!(s.contents(Some(&etag)).not_modified);
        s.create_view("v", "error").unwrap();
        let changed = s.contents(Some(&etag));
        assert!(!changed.not_modified);
        assert_ne!(changed.etag.unwrap(), etag);
    }

    #[test]
    fn snapshot_items_equal_the_per_schema_answers() {
        let s = Session::new(StoredCatalog::new("mem"));
        s.create_view("v", "error").unwrap();
        s.create_table(&["main"], "t").unwrap();
        s.create_schema(&["s2"]).unwrap();
        let c = s.contents(None);
        let paths: Vec<_> = c.schemas.iter().map(|s| s.path.clone()).collect();
        assert_eq!(paths, vec![path(&["main"]), path(&["s2"])]);
        let tables = |schema: &[&str]| {
            let batch = wire::to_batch(p::CatalogSchemaContentsTablesParams {
                attach_opaque_data: Bytes::from(b"attach".to_vec()),
                path: path(schema),
                transaction_opaque_data: None,
            })
            .unwrap();
            let r: ItemsResult = wire::from_batch(&inner(
                s.call("catalog_schema_contents_tables", batch)
                    .unwrap()
                    .unwrap(),
            ))
            .unwrap();
            r.items
        };
        assert_eq!(c.schemas[0].tables, tables(&["main"]));
        assert_eq!(c.schemas[0].views.len(), 1);
        let info: SchemaInfo =
            wire::from_batch(&crate::ipc::read_batch(&c.schemas[1].schema.0).unwrap()).unwrap();
        assert_eq!(info.comment.as_deref(), Some("made here"));
    }

    #[test]
    fn unsupported_ddl_is_refused_by_name() {
        let s = Session::new(StoredCatalog::new("mem"));
        let batch = wire::to_batch(p::CatalogViewDropParams {
            attach_opaque_data: Bytes::from(b"attach".to_vec()),
            schema_path: path(&["main"]),
            name: "x".to_string(),
            ignore_not_found: true,
            cascade: false,
            transaction_opaque_data: None,
        })
        .unwrap();
        let err = s
            .call("catalog_view_rename", batch)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not support catalog_view_rename"),
            "{err}"
        );
    }
    /// Through the dispatcher: attach by name mints a private session per
    /// ATTACH, `catalog_catalogs` advertises the catalog, and every catalog RPC
    /// carrying its handle is routed to it (not to the declarative primary).
    #[test]
    fn dispatcher_routes_attaches_and_rpcs() {
        use crate::dispatch::Dispatcher;
        use crate::protocol::dtos::CatalogInfo;
        let mut d = Dispatcher::new("example");
        d.register_stored_catalog(StoredCatalog::new("mem").unversioned());
        let attach = || {
            let inner = wire::to_batch(attach_request()).unwrap();
            let batch = wire::to_batch(p::CatalogAttachParams {
                request: Bytes::from(crate::ipc::write_batch(&inner).unwrap()),
            })
            .unwrap();
            let r: CatalogAttachResult = wire::from_batch(&inner_of(
                d.handle_catalog_attach(&request("catalog_attach", batch))
                    .unwrap()
                    .unwrap(),
            ))
            .unwrap();
            r
        };
        let a = attach();
        let b = attach();
        assert_ne!(a.attach_opaque_data.0, b.attach_opaque_data.0);
        assert!(a.supports_catalog_contents);
        assert!(!a.supports_transactions);
        assert_eq!(a.catalog_version, 0);

        let view = |handle: &Bytes, name: &str| {
            let batch = wire::to_batch(p::CatalogViewCreateParams {
                attach_opaque_data: handle.clone(),
                schema_path: path(&["main"]),
                name: name.to_string(),
                definition: "SELECT 1".to_string(),
                on_conflict: DictString("error".to_string()),
                transaction_opaque_data: None,
            })
            .unwrap();
            let req = request("catalog_view_create", batch);
            d.stored_or(&req, || d.handle_read_only(&req))
        };
        view(&a.attach_opaque_data, "va").unwrap();
        let views = |handle: &Bytes| -> Vec<String> {
            let batch = wire::to_batch(p::CatalogSchemaContentsViewsParams {
                attach_opaque_data: handle.clone(),
                path: path(&["main"]),
                transaction_opaque_data: None,
            })
            .unwrap();
            let req = request("catalog_schema_contents_views", batch);
            let r: ItemsResult = wire::from_batch(&inner_of(
                d.stored_or(&req, || d.handle_contents_views(&req))
                    .unwrap()
                    .unwrap(),
            ))
            .unwrap();
            r.items
                .iter()
                .map(|i| {
                    let v: ViewInfo =
                        wire::from_batch(&crate::ipc::read_batch(&i.0).unwrap()).unwrap();
                    v.name
                })
                .collect()
        };
        assert_eq!(views(&a.attach_opaque_data), vec!["va".to_string()]);
        assert!(
            views(&b.attach_opaque_data).is_empty(),
            "attaches are private"
        );
        // The declarative primary is still read-only.
        let err = view(&Bytes::from(b"example".to_vec()), "x").unwrap_err();
        assert!(err.to_string().contains("read-only"));

        let catalogs = d
            .handle_catalog_catalogs(&request(
                "catalog_catalogs",
                RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty())),
            ))
            .unwrap()
            .unwrap();
        let r: ItemsResult = wire::from_batch(&inner_of(catalogs)).unwrap();
        let names: Vec<String> = r
            .items
            .iter()
            .map(|i| {
                let c: CatalogInfo =
                    wire::from_batch(&crate::ipc::read_batch(&i.0).unwrap()).unwrap();
                c.name
            })
            .collect();
        assert!(names.contains(&"mem".to_string()), "{names:?}");
    }

    fn inner_of(result: RecordBatch) -> RecordBatch {
        inner(result)
    }
}
