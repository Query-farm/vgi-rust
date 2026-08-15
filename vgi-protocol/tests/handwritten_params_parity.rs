// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The worker's hand-written params structs must match the advertised schema.
//!
//! `protocol::dtos` carries hand-written `*Params` structs that the **worker
//! reads requests with**; `generated::request_params` carries generated ones a
//! **client builds requests with**. Both are checked here against
//! `wire::params_schema_for` — the schema the worker advertises through
//! `__describe__` — so all three agree.
//!
//! This is not redundant with the generated `schema_parity` module. That one
//! proves the *client* side is right; this proves the *worker* side is. It is
//! also the check that would have caught the `r#type` bug independently: the
//! derive used to look up `column_by_name("r#type")` while every canonical peer
//! sends a column named `type`, so `catalog_schema_contents_functions` and
//! `_macros` failed for any Python or C++ caller.

use vgi_protocol::protocol::dtos::{
    CatalogDetachParams, CatalogSchemaContentsFunctionsParams, CatalogSchemaNameParams,
    CatalogSchemasParams, CatalogTransactionBeginParams, CatalogTransactionEndParams,
    CatalogVersionParams,
};
use vgi_protocol::wire::{flat_schema, params_schema_for};

#[test]
fn worker_read_structs_match_the_advertised_schema() {
    assert_eq!(
        flat_schema::<CatalogSchemasParams>(),
        params_schema_for("catalog_schemas"),
        "CatalogSchemasParams",
    );
    assert_eq!(
        flat_schema::<CatalogDetachParams>(),
        params_schema_for("catalog_detach"),
        "CatalogDetachParams",
    );
    assert_eq!(
        flat_schema::<CatalogVersionParams>(),
        params_schema_for("catalog_version"),
        "CatalogVersionParams",
    );
    assert_eq!(
        flat_schema::<CatalogTransactionBeginParams>(),
        params_schema_for("catalog_transaction_begin"),
        "CatalogTransactionBeginParams",
    );
}

#[test]
fn one_struct_serves_several_methods_only_if_their_schemas_agree() {
    // `CatalogSchemaNameParams` is documented as covering `catalog_schema_get`
    // plus the `_contents_{tables,views,indexes}` family. That reuse is only
    // sound while those four advertise identical params.
    for method in [
        "catalog_schema_get",
        "catalog_schema_contents_tables",
        "catalog_schema_contents_views",
        "catalog_schema_contents_indexes",
    ] {
        assert_eq!(
            flat_schema::<CatalogSchemaNameParams>(),
            params_schema_for(method),
            "CatalogSchemaNameParams is reused for '{method}' but their schemas differ",
        );
    }

    // Likewise the functions/macros pair, which additionally carries `type`.
    for method in [
        "catalog_schema_contents_functions",
        "catalog_schema_contents_macros",
    ] {
        assert_eq!(
            flat_schema::<CatalogSchemaContentsFunctionsParams>(),
            params_schema_for(method),
            "CatalogSchemaContentsFunctionsParams is reused for '{method}' but their schemas differ",
        );
    }
}

#[test]
fn the_type_column_is_not_spelled_with_a_raw_identifier_prefix() {
    // The regression that motivated this file: a `r#type` field must reach the
    // wire as `type`, or no canonical peer can call the method.
    let schema = flat_schema::<CatalogSchemaContentsFunctionsParams>();
    assert!(
        schema.field_with_name("type").is_ok(),
        "expected a `type` column, got {schema:?}",
    );
    assert!(
        schema.field_with_name("r#type").is_err(),
        "the raw-identifier prefix leaked into the wire column name",
    );
}

#[test]
fn transaction_end_params_require_a_transaction_handle() {
    // commit/rollback both take a non-null transaction handle, unlike the
    // read methods where it is optional. A client that sends null here is
    // asking the worker to end "no transaction".
    let schema = flat_schema::<CatalogTransactionEndParams>();
    let txn = schema
        .field_with_name("transaction_opaque_data")
        .expect("transaction_opaque_data column");
    assert!(!txn.is_nullable(), "commit/rollback need a real handle");

    for method in ["catalog_transaction_commit", "catalog_transaction_rollback"] {
        assert_eq!(
            schema,
            params_schema_for(method),
            "CatalogTransactionEndParams does not match '{method}'",
        );
    }
}
