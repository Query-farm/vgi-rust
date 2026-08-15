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
    CatalogInfo, FunctionInfo, MacroInfo, SchemaInfo, TableInfo, ViewInfo,
};
use vgi_protocol::wire::flat_schema;

#[test]
fn schema_info_matches() {
    assert_eq!(flat_schema::<SchemaInfo>(), gen::schema_info_schema());
}

/// The two `InlineI64` columns on `TableInfo`, which are deliberately built
/// nullable and tightened to non-null on serialization.
const INLINE_I64_COLUMNS: &[&str] = &["cardinality_estimate", "cardinality_max"];

#[test]
fn table_info_matches_apart_from_the_inline_i64_columns() {
    // `TableInfo` is the one DTO whose derived schema is *intentionally* not
    // the wire schema. `InlineI64::nullable()` returns true so Arrow will
    // accept a NULL child when building the StructArray; `serialize_items`
    // then tightens those columns back to non-null, because the C++
    // extension's result-schema check requires `int64 not null` while still
    // reading NULL as "not inlined, fire the RPC". See the `InlineI64` docs
    // in `dtos.rs`.
    //
    // So this test asserts the delta is *exactly* those two columns and
    // *exactly* nullability — anything else is real drift.
    let derived = flat_schema::<TableInfo>();
    let canonical = gen::table_info_schema();

    let derived_names: Vec<&str> = derived.fields().iter().map(|f| f.name().as_str()).collect();
    let canonical_names: Vec<&str> = canonical
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(derived_names, canonical_names, "field set or order drifted");

    for (d, c) in derived.fields().iter().zip(canonical.fields().iter()) {
        assert_eq!(d.data_type(), c.data_type(), "type drift on `{}`", d.name());
        if INLINE_I64_COLUMNS.contains(&d.name().as_str()) {
            assert!(d.is_nullable(), "`{}` must build nullable", d.name());
            assert!(
                !c.is_nullable(),
                "`{}` must be non-null on the wire",
                c.name()
            );
        } else {
            assert_eq!(
                d.is_nullable(),
                c.is_nullable(),
                "nullability drift on `{}`",
                d.name()
            );
        }
    }
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
