// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Host `vgi.v2` on an [`RpcServer`]: the [`Dispatcher`]'s implementation of the
//! generated [`VgiService`] trait.
//!
//! The method table itself — every method's name, method type and params /
//! result / header schemas — is generated from the reference
//! ([`super::vgi_service::register`]). This module only says what each method
//! *does*: open the sealed opaque values a request carries, route a catalog RPC
//! to the stored catalog its attach names, and call the handler. A method not
//! overridden here keeps the generated `UNIMPLEMENTED` default.

use std::sync::Arc;

use arrow_array::RecordBatch;
use vgi_rpc::stream::StreamStateKind;
use vgi_rpc::{CallContext, Request, Result, RpcServer, StreamResult};

use crate::dispatch::Dispatcher;

pub use super::vgi_service::{not_implemented, VgiService};

/// Register all VGI methods against `srv`, backed by `disp`.
pub fn register(srv: &mut RpcServer, disp: Arc<Dispatcher>) {
    super::vgi_service::register(srv, disp);
}

/// `vgi.v2` methods this SDK registers but does not implement.
///
/// The protocol is the unit of optionality: every SDK hosts every `vgi.v2`
/// method with the reference's schemas, so reflection reports one protocol
/// hash everywhere. These keep the generated [`VgiService`] default and
/// answer every call with `UNIMPLEMENTED` / `method_not_implemented` -- never
/// a silent success.
///
/// The DML getters resolve the INSERT/UPDATE/DELETE function of a writable
/// table; this port's catalogs are read-only for DML.
pub const UNIMPLEMENTED_METHODS: &[&str] = &[
    "catalog_table_insert_function_get",
    "catalog_table_update_function_get",
    "catalog_table_delete_function_get",
];

type Answer = Result<Option<RecordBatch>>;

impl Dispatcher {
    /// A catalog RPC: opened, then served by the stored catalog its attach
    /// names, or by `handler` for the declarative catalogs.
    fn catalog_rpc(
        &self,
        req: &Request,
        ctx: &CallContext,
        handler: impl FnOnce(&Self, &Request) -> Answer,
    ) -> Answer {
        self.with_opened(req, ctx, |req| self.stored_or(req, || handler(self, req)))
    }
}

/// Methods served by [`Dispatcher::catalog_rpc`].
macro_rules! catalog_rpcs {
    ($($method:ident => $handler:ident),* $(,)?) => {$(
        fn $method(&self, req: &Request, ctx: &CallContext) -> Answer {
            self.catalog_rpc(req, ctx, Self::$handler)
        }
    )*};
}

/// Methods whose handler needs only the opened request.
macro_rules! opened_rpcs {
    ($($method:ident => $handler:ident),* $(,)?) => {$(
        fn $method(&self, req: &Request, ctx: &CallContext) -> Answer {
            self.with_opened(req, ctx, |req| self.$handler(req))
        }
    )*};
}

/// Methods whose handler needs the opened request and the call context.
macro_rules! opened_ctx_rpcs {
    ($($method:ident => $handler:ident),* $(,)?) => {$(
        fn $method(&self, req: &Request, ctx: &CallContext) -> Answer {
            self.with_opened(req, ctx, |req| self.$handler(req, ctx))
        }
    )*};
}

impl VgiService for Dispatcher {
    // --- core: bind (unary) + init (dynamic stream) ---
    opened_ctx_rpcs! {
        bind => handle_bind,
    }

    fn init(&self, req: &Request, ctx: &CallContext) -> Result<StreamResult> {
        self.with_opened(req, ctx, |req| self.handle_init(req, ctx))
    }

    /// HTTP continuations rebuild the (stateless) exchange handler from an
    /// AEAD state token.
    fn decode_init_state(&self, state: &[u8]) -> Result<StreamStateKind> {
        self.decode_init_exchange_state(state)
    }

    // --- catalog handshake: the attach and transaction handles a result
    //     mints are sealed for the caller ---
    fn catalog_attach(&self, req: &Request, ctx: &CallContext) -> Answer {
        let result = self.with_opened(req, ctx, |opened| {
            self.handle_catalog_attach_as(opened, &ctx.auth)
        })?;
        self.seal_result(req, ctx, result)
    }

    fn catalog_transaction_begin(&self, req: &Request, ctx: &CallContext) -> Answer {
        let result = self.catalog_rpc(req, ctx, Self::handle_transaction_begin)?;
        self.seal_result(req, ctx, result)
    }

    catalog_rpcs! {
        catalog_version => handle_catalog_version,
        catalog_transaction_commit => handle_void,
        catalog_transaction_rollback => handle_void,
        catalog_detach => handle_void,
    }

    // --- aggregates ---
    opened_ctx_rpcs! {
        aggregate_bind => handle_aggregate_bind,
    }
    opened_rpcs! {
        aggregate_update => handle_aggregate_update,
        aggregate_combine => handle_aggregate_combine,
        aggregate_finalize => handle_aggregate_finalize,
        aggregate_destructor => handle_aggregate_destructor,
        aggregate_window_init => handle_aggregate_window_init,
        aggregate_window => handle_aggregate_window,
        aggregate_window_batch => handle_aggregate_window_batch,
        aggregate_window_destructor => handle_aggregate_window_destructor,
        aggregate_streaming_open => handle_aggregate_streaming_open,
        aggregate_streaming_chunk => handle_aggregate_streaming_chunk,
        aggregate_streaming_close => handle_aggregate_streaming_close,
    }

    // --- table buffering ---
    opened_ctx_rpcs! {
        table_buffering_process => handle_buffering_process,
        table_buffering_combine => handle_buffering_combine,
    }
    opened_rpcs! {
        table_buffering_destructor => handle_buffering_destructor,
    }

    // --- table functions ---
    opened_ctx_rpcs! {
        table_function_statistics => handle_table_function_statistics,
        table_function_cardinality => handle_table_function_cardinality,
        table_function_plan => handle_table_function_plan,
    }
    opened_rpcs! {
        table_function_dynamic_to_string => handle_table_function_dynamic_to_string,
        catalog_catalogs => handle_catalog_catalogs,
    }

    // --- catalog-mutating DDL: served by a stored (DDL-capable) catalog; for
    //     a declarative catalog accepted (pins the wire contract) then
    //     rejected with `catalog is read-only` ---
    catalog_rpcs! {
        catalog_create => handle_read_only,
        catalog_drop => handle_read_only,
        catalog_schema_create => handle_read_only,
        catalog_schema_drop => handle_read_only,
        catalog_table_create => handle_read_only,
        catalog_table_drop => handle_read_only,
        catalog_table_rename => handle_read_only,
        catalog_table_comment_set => handle_read_only,
        catalog_table_column_add => handle_read_only,
        catalog_table_column_drop => handle_read_only,
        catalog_table_column_rename => handle_read_only,
        catalog_table_column_type_change => handle_read_only,
        catalog_table_column_default_set => handle_read_only,
        catalog_table_column_default_drop => handle_read_only,
        catalog_table_column_comment_set => handle_read_only,
        catalog_table_not_null_set => handle_read_only,
        catalog_table_not_null_drop => handle_read_only,
        catalog_view_create => handle_read_only,
        catalog_view_drop => handle_read_only,
        catalog_view_rename => handle_read_only,
        catalog_view_comment_set => handle_read_only,
        catalog_macro_create => handle_read_only,
        catalog_macro_drop => handle_read_only,
        catalog_index_create => handle_read_only,
        catalog_index_drop => handle_read_only,
    }

    // --- schema discovery ---
    catalog_rpcs! {
        catalog_schemas => handle_catalog_schemas,
        // Protocol 2.1.0: the whole catalog in one call. The client calls it
        // only when `catalog_attach` advertises `supports_catalog_contents`.
        catalog_contents => handle_catalog_contents,
        catalog_schema_get => handle_schema_get,
        catalog_schema_contents_functions => handle_contents_functions,
        catalog_schema_contents_views => handle_contents_views,
        catalog_schema_contents_macros => handle_contents_macros,
        catalog_schema_contents_tables => handle_contents_tables,
        catalog_table_get => handle_table_get,
        catalog_table_scan_function_get => handle_table_scan_function_get,
        catalog_table_scan_branches_get => handle_table_scan_branches_get,
        catalog_table_column_statistics_get => handle_table_column_statistics_get,
        catalog_copy_from_formats => handle_catalog_copy_from_formats,
        // Discovery a declarative catalog answers with an empty listing.
        catalog_schema_contents_indexes => handle_empty_items,
        catalog_view_get => handle_empty_items,
        catalog_macro_get => handle_empty_items,
        catalog_index_get => handle_empty_items,
    }
}
