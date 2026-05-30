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
        srv.register_unary("bind", wire::result_binary_schema(), move |req, ctx| {
            d.handle_bind(req, ctx)
        });
    }
    {
        let d = disp.clone();
        let dd = disp.clone();
        let empty = Arc::new(arrow_schema::Schema::empty());
        let info = vgi_rpc::MethodInfo::stream(
            "init",
            MethodType::Dynamic,
            empty,
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
        srv.register_unary(
            "catalog_attach",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_attach(req),
        );
    }
    {
        let d = disp.clone();
        srv.register_unary(
            "catalog_version",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_version(req),
        );
    }
    {
        let d = disp.clone();
        srv.register_unary(
            "catalog_transaction_begin",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_transaction_begin(req),
        );
    }
    // --- aggregates ---
    {
        let d = disp.clone();
        srv.register_unary("aggregate_bind", wire::result_binary_schema(), move |req, ctx| {
            d.handle_aggregate_bind(req, ctx)
        });
    }
    {
        let d = disp.clone();
srv.register_unary("aggregate_update", wire::result_binary_schema(), move |req, _ctx| d.handle_aggregate_update(req));
    }
    {
        let d = disp.clone();
srv.register_unary("aggregate_combine", wire::result_binary_schema(), move |req, _ctx| d.handle_aggregate_combine(req));
    }
    {
        let d = disp.clone();
        srv.register_unary("aggregate_finalize", wire::result_binary_schema(), move |req, _ctx| {
            d.handle_aggregate_finalize(req)
        });
    }
    {
        let d = disp.clone();
srv.register_unary("aggregate_destructor", wire::result_binary_schema(), move |req, _ctx| d.handle_aggregate_destructor(req));
    }

    // --- table buffering ---
    {
        let d = disp.clone();
        srv.register_unary(
            "table_buffering_process",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_buffering_process(req),
        );
    }
    {
        let d = disp.clone();
        srv.register_unary(
            "table_buffering_combine",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_buffering_combine(req),
        );
    }
    {
        let d = disp.clone();
        let empty = Arc::new(arrow_schema::Schema::empty());
        srv.register_unary("table_buffering_destructor", empty, move |req, _ctx| {
            d.handle_buffering_destructor(req)
        });
    }

    register_void(srv, &disp, "catalog_transaction_commit");
    register_void(srv, &disp, "catalog_transaction_rollback");
    register_void(srv, &disp, "catalog_detach");

    // --- schema discovery ---
    {
        let d = disp.clone();
        srv.register_unary(
            "catalog_schemas",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_catalog_schemas(req),
        );
    }
    {
        let d = disp.clone();
        srv.register_unary(
            "catalog_schema_get",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_schema_get(req),
        );
    }
    {
        let d = disp.clone();
        srv.register_unary(
            "catalog_schema_contents_functions",
            wire::result_binary_schema(),
            move |req, _ctx| d.handle_contents_functions(req),
        );
    }

    {
        let d = disp.clone();
        srv.register_unary("catalog_schema_contents_views", wire::result_binary_schema(), move |req, _ctx| {
            d.handle_contents_views(req)
        });
    }
    {
        let d = disp.clone();
        srv.register_unary("catalog_schema_contents_macros", wire::result_binary_schema(), move |req, _ctx| {
            d.handle_contents_macros(req)
        });
    }
    {
        let d = disp.clone();
        srv.register_unary("catalog_schema_contents_tables", wire::result_binary_schema(), move |req, _ctx| {
            d.handle_contents_tables(req)
        });
    }
    {
        let d = disp.clone();
        srv.register_unary("catalog_table_get", wire::result_binary_schema(), move |req, _ctx| {
            d.handle_table_get(req)
        });
    }

    // --- discovery methods that return empty lists for now ---
    for name in [
        "catalog_catalogs",
        "catalog_schema_contents_indexes",
        "catalog_view_get",
        "catalog_macro_get",
        "catalog_index_get",
    ] {
        let d = disp.clone();
        srv.register_unary(name, wire::result_binary_schema(), move |req, _ctx| {
            d.handle_empty_items(req)
        });
    }
}

fn register_void(srv: &mut RpcServer, disp: &Arc<Dispatcher>, name: &str) {
    let d = disp.clone();
    let empty = Arc::new(arrow_schema::Schema::empty());
    srv.register_unary(name.to_string(), empty, move |req, _ctx| d.handle_void(req));
}
