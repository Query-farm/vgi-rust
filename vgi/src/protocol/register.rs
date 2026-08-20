// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Wire every VGI RPC method onto an [`RpcServer`], delegating to a shared
//! [`Dispatcher`].

use std::sync::Arc;

use vgi_rpc::{MethodType, RpcServer};

use crate::dispatch::Dispatcher;

use crate::wire;

/// Register all VGI methods against `srv`, backed by `disp`.
pub fn register(srv: &mut RpcServer, disp: Arc<Dispatcher>) {
    // --- core: bind (unary) + init (dynamic stream) ---
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "bind",
            wire::params_schema_for("bind"),
            wire::result_binary_schema(),
            move |req, ctx| d.handle_bind(req, ctx),
        ));
    }
    {
        let d = disp.clone();
        let dd = disp.clone();
        // `init` declares the same boxed `{request: binary}` params schema as
        // every other method. It used to register `Schema::empty()` — harmless
        // while nothing validated it, but vgi-rpc now enforces the declared
        // parameter contract before dispatch, and an empty declaration means
        // "this method takes no columns", which rejects the boxed request.
        let info = vgi_rpc::MethodInfo::stream(
            "init",
            MethodType::Dynamic,
            wire::params_schema_for("init"),
            move |req, ctx| d.handle_init(req, ctx),
        )
            // HTTP continuations rebuild the (stateless) exchange handler from an
            // AEAD state token; without a decoder the server 500s on /init/exchange.
            .with_state_decoder(Arc::new(move |bytes: &[u8]| dd.decode_init_state(bytes)));
        srv.register(info);
    }

    // --- catalog handshake ---
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_attach",
            wire::params_schema_for("catalog_attach"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_attach(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_version",
            wire::params_schema_for("catalog_version"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_version(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_transaction_begin",
            wire::params_schema_for("catalog_transaction_begin"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_transaction_begin(req),
        ));
    }
    // --- aggregates ---
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_bind",
            wire::params_schema_for("aggregate_bind"),
            wire::result_binary_schema(),
            move |req, ctx| d.handle_aggregate_bind(req, ctx),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_update",
            wire::params_schema_for("aggregate_update"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_update(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_combine",
            wire::params_schema_for("aggregate_combine"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_combine(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_finalize",
            wire::params_schema_for("aggregate_finalize"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_finalize(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_destructor",
            wire::params_schema_for("aggregate_destructor"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_destructor(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_window_init",
            wire::params_schema_for("aggregate_window_init"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_window_init(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_window",
            wire::params_schema_for("aggregate_window"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_window(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_window_batch",
            wire::params_schema_for("aggregate_window_batch"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_window_batch(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_window_destructor",
            wire::params_schema_for("aggregate_window_destructor"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_window_destructor(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_streaming_open",
            wire::params_schema_for("aggregate_streaming_open"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_streaming_open(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_streaming_chunk",
            wire::params_schema_for("aggregate_streaming_chunk"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_streaming_chunk(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "aggregate_streaming_close",
            wire::params_schema_for("aggregate_streaming_close"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_aggregate_streaming_close(req),
        ));
    }

    // --- table buffering ---
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "table_buffering_process",
            wire::params_schema_for("table_buffering_process"),
            wire::result_binary_schema(),
            move |req, ctx| d.handle_buffering_process(req, ctx),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "table_buffering_combine",
            wire::params_schema_for("table_buffering_combine"),
            wire::result_binary_schema(),
            move |req, ctx| d.handle_buffering_combine(req, ctx),
        ));
    }
    {
        let d = disp.clone();
        let empty = Arc::new(arrow_schema::Schema::empty());
        srv.register(vgi_rpc::MethodInfo::unary(
            "table_buffering_destructor",
            wire::params_schema_for("table_buffering_destructor"),
            empty,
            move |req, _ctx| d.handle_buffering_destructor(req),
        ));
    }

    register_void(srv, &disp, "catalog_transaction_commit");
    register_void(srv, &disp, "catalog_transaction_rollback");
    register_void(srv, &disp, "catalog_detach");

    // --- catalog-mutating DDL: accepted (pins the wire contract) then
    //     rejected with `catalog is read-only` (the example catalog is r/o) ---
    for name in [
        "catalog_create",
        "catalog_drop",
        "catalog_schema_create",
        "catalog_schema_drop",
        "catalog_table_create",
        "catalog_table_drop",
        "catalog_table_rename",
        "catalog_table_comment_set",
        "catalog_table_column_add",
        "catalog_table_column_drop",
        "catalog_table_column_rename",
        "catalog_table_column_type_change",
        "catalog_table_column_default_set",
        "catalog_table_column_default_drop",
        "catalog_table_column_comment_set",
        "catalog_table_not_null_set",
        "catalog_table_not_null_drop",
        "catalog_view_create",
        "catalog_view_drop",
        "catalog_view_rename",
        "catalog_view_comment_set",
        "catalog_macro_create",
        "catalog_macro_drop",
        "catalog_index_create",
        "catalog_index_drop",
    ] {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            name,
            wire::params_schema_for(name),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_read_only(req),
        ));
    }

    // --- schema discovery ---
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_schemas",
            wire::params_schema_for("catalog_schemas"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_schemas(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_schema_get",
            wire::params_schema_for("catalog_schema_get"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_schema_get(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_schema_contents_functions",
            wire::params_schema_for("catalog_schema_contents_functions"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_contents_functions(req),
        ));
    }

    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_schema_contents_views",
            wire::params_schema_for("catalog_schema_contents_views"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_contents_views(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_schema_contents_macros",
            wire::params_schema_for("catalog_schema_contents_macros"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_contents_macros(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_schema_contents_tables",
            wire::params_schema_for("catalog_schema_contents_tables"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_contents_tables(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_table_get",
            wire::params_schema_for("catalog_table_get"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_table_get(req),
        ));
    }
    {
        // Legacy scan-function resolution for non-inlined function-backed
        // tables. The response is a FLAT `ScanFunctionResult` batch (not the
        // `{result: binary}` envelope), matching the C++ extractor.
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_table_scan_function_get",
            wire::params_schema_for("catalog_table_scan_function_get"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_table_scan_function_get(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_table_scan_branches_get",
            wire::params_schema_for("catalog_table_scan_branches_get"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_table_scan_branches_get(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_table_column_statistics_get",
            wire::params_schema_for("catalog_table_column_statistics_get"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_table_column_statistics_get(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "table_function_statistics",
            wire::params_schema_for("table_function_statistics"),
            wire::result_binary_schema(),
            move |req, ctx| d.handle_table_function_statistics(req, ctx),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "table_function_cardinality",
            wire::params_schema_for("table_function_cardinality"),
            wire::result_binary_schema(),
            move |req, ctx| d.handle_table_function_cardinality(req, ctx),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "table_function_plan",
            wire::params_schema_for("table_function_plan"),
            wire::result_binary_schema(),
            move |req, ctx| d.handle_table_function_plan(req, ctx),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "table_function_dynamic_to_string",
            wire::params_schema_for("table_function_dynamic_to_string"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_table_function_dynamic_to_string(req),
        ));
    }

    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_catalogs",
            wire::params_schema_for("catalog_catalogs"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_catalogs(req),
        ));
    }
    {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            "catalog_copy_from_formats",
            wire::params_schema_for("catalog_copy_from_formats"),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_copy_from_formats(req),
        ));
    }

    // --- discovery methods that return empty lists for now ---
    for name in [
        "catalog_schema_contents_indexes",
        "catalog_view_get",
        "catalog_macro_get",
        "catalog_index_get",
    ] {
        let d = disp.clone();
        srv.register(vgi_rpc::MethodInfo::unary(
            name,
            wire::params_schema_for(name),
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_empty_items(req),
        ));
    }
}

fn register_void(srv: &mut RpcServer, disp: &Arc<Dispatcher>, name: &str) {
    let d = disp.clone();
    let empty = Arc::new(arrow_schema::Schema::empty());
    srv.register(vgi_rpc::MethodInfo::unary(
        name.to_string(),
        wire::params_schema_for(name),
        empty,
        move |req, _ctx| d.handle_void(req),
    ));
}
