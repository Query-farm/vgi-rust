// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Hand-written catalog item DTOs must match their generated schemas.
//!
//! The item structs (`SchemaInfo`, `TableInfo`, `FunctionInfo`, …) are what a
//! worker serializes into `ItemsResult.items` and what a client deserializes
//! back out. Their shape is fixed by the canonical Python protocol, and the
//! generated factories in `generated::protocol_schemas` are that shape
//! transcribed. If a hand-written DTO drifts, a client decodes garbage or fails
//! on a missing column — so pin them together.
//!
//! Field **order** matters as well as membership: the C++ extension reads
//! several result schemas positionally, as `dtos.rs` notes at the top.

use vgi_protocol::generated::protocol_schemas as gen;
use vgi_protocol::protocol::dtos::{
    CatalogInfo, FunctionInfo, MacroInfo, ScanBranch, ScanFunctionResult, SchemaInfo, TableInfo,
    ViewInfo,
};
use vgi_protocol::wire::flat_schema;

#[test]
fn schema_info_matches() {
    assert_eq!(flat_schema::<SchemaInfo>(), gen::schema_info_schema());
}

#[test]
fn table_info_matches() {
    // `TableInfo` used to be the one DTO whose derived schema was
    // *intentionally* not the wire schema: `cardinality_estimate` /
    // `cardinality_max` were built nullable (so Arrow accepts a NULL child)
    // and then tightened back to non-null on serialization, because the
    // canonical schema declared them `int64 not null` while every peer wrote
    // NULL into them for "not inlined, fire the RPC".
    //
    // That non-null declaration was the `Annotated[X | None, ...]` derivation
    // bug in vgi-rpc; the canonical schema now declares them nullable, the
    // tightening is gone, and the derived schema is simply the wire schema.
    assert_eq!(flat_schema::<TableInfo>(), gen::table_info_schema());
}

#[test]
fn view_info_matches() {
    assert_eq!(flat_schema::<ViewInfo>(), gen::view_info_schema());
}

#[test]
fn macro_info_matches() {
    assert_eq!(flat_schema::<MacroInfo>(), gen::macro_info_schema());
}

#[test]
fn function_info_matches() {
    assert_eq!(flat_schema::<FunctionInfo>(), gen::function_info_schema());
}

#[test]
fn catalog_info_matches() {
    // `CatalogInfo` had no Rust DTO until a client needed to *read* one —
    // the worker builds it by hand in `vgi::catalog::serialize_catalog_info`.
    // This is the check that the reader and that hand-builder agree.
    assert_eq!(flat_schema::<CatalogInfo>(), gen::catalog_info_schema());
}

#[test]
fn scan_function_result_matches() {
    // Nothing else in this suite covered `ScanFunctionResult` before protocol
    // 1.5.0 added `schema_name` — that gap is exactly how a missed field here
    // could have shipped silently. Pin it now like every other DTO above.
    assert_eq!(
        flat_schema::<ScanFunctionResult>(),
        gen::scan_function_result_schema()
    );
}

#[test]
fn scan_branch_matches() {
    assert_eq!(flat_schema::<ScanBranch>(), gen::scan_branch_schema());
}
