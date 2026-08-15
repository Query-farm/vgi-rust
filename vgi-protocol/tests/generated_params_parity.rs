// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The generated params-schema table versus the hand-written one it replaced.
//!
//! Before code generation, `wire::params_schema_for` carried a hand-maintained
//! `match` naming 13 methods; every other method fell through to the wrapped
//! `request` envelope. Generating the table from the canonical Python
//! `VgiProtocol` changed two things, and this test pins both:
//!
//! 1. **Parity** — for the methods the old table named, the generated schema is
//!    identical. That is what makes replacing it safe.
//! 2. **Two corrections and 32 additions** — the old table was not merely
//!    incomplete, it was wrong in two places. See the tests below.
//!
//! `__describe__` advertises these schemas, and a client that builds its request
//! from the advertised schema (the TypeScript client does) sends a
//! metadata-only batch when the schema is wrong — every handler then reports a
//! missing column. So these are live-wire bugs, not cosmetic drift.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_protocol::wire::{params_schema_for, request_binary_schema};

/// The table exactly as it stood before code generation (renamed only).
#[rustfmt::skip]
fn legacy_params_schema_for(method: &str) -> SchemaRef {
    match method {
        "catalog_copy_from_formats" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_schema_contents_functions" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_schema_contents_macros" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_schema_contents_tables" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_schema_contents_views" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_schema_get" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_schemas" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_table_column_statistics_get" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_table_get" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("at_unit", DataType::Utf8, true),
            Field::new("at_value", DataType::Utf8, true),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_table_scan_branches_get" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("at_unit", DataType::Utf8, true),
            Field::new("at_value", DataType::Utf8, true),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_table_scan_function_get" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("at_unit", DataType::Utf8, true),
            Field::new("at_value", DataType::Utf8, true),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        "catalog_transaction_begin" => Arc::new(Schema::new(vec![Field::new(
            "attach_opaque_data",
            DataType::Binary,
            false,
        )])),
        "catalog_version" => Arc::new(Schema::new(vec![
            Field::new("attach_opaque_data", DataType::Binary, false),
            Field::new("transaction_opaque_data", DataType::Binary, true),
        ])),
        // Everything else carried the wrapped `request` envelope.
        _ => request_binary_schema(),
    }
}

/// Methods the old table named and got right — the generated table must agree.
const UNCHANGED: &[&str] = &[
    "catalog_copy_from_formats",
    "catalog_schema_contents_tables",
    "catalog_schema_contents_views",
    "catalog_schema_get",
    "catalog_schemas",
    "catalog_table_column_statistics_get",
    "catalog_table_get",
    "catalog_table_scan_branches_get",
    "catalog_table_scan_function_get",
    "catalog_transaction_begin",
    "catalog_version",
];

/// Methods the old table named and got **wrong** — both omitted the `type`
/// column that selects which kind of function/macro to list.
const CORRECTED: &[&str] = &[
    "catalog_schema_contents_functions",
    "catalog_schema_contents_macros",
];

#[test]
fn generated_table_matches_the_hand_written_one() {
    for method in UNCHANGED {
        assert_eq!(
            params_schema_for(method),
            legacy_params_schema_for(method),
            "generated params schema for '{method}' diverges from the hand-written table it replaced",
        );
    }
}

#[test]
fn generation_restored_the_type_column_the_old_table_dropped() {
    for method in CORRECTED {
        let generated = params_schema_for(method);
        let legacy = legacy_params_schema_for(method);
        assert_ne!(
            generated, legacy,
            "'{method}' was expected to differ from the old table — did the protocol change?",
        );
        assert!(
            generated.field_with_name("type").is_ok(),
            "'{method}' must advertise the `type` column that selects the kind to list; got {generated:?}",
        );
        assert!(
            legacy.field_with_name("type").is_err(),
            "the frozen legacy table was supposed to be missing `type` — this fixture is stale",
        );
    }
}

#[test]
fn unknown_methods_still_fall_back_to_the_envelope() {
    // A peer speaking a newer protocol must not blow up; the wrapped `request`
    // envelope is the safe assumption, exactly as before.
    assert_eq!(
        params_schema_for("some_method_from_the_future"),
        request_binary_schema(),
    );
}

#[test]
fn methods_the_old_table_never_named_now_advertise_real_params() {
    // 32 methods took flat params but fell through to the envelope. Spot-check
    // one: a client reading the advertised schema previously saw `request:
    // binary` and had no way to learn the real field list.
    let schema = params_schema_for("catalog_index_get");
    assert_ne!(
        schema,
        request_binary_schema(),
        "catalog_index_get takes flat params and must not advertise the envelope",
    );
    assert_eq!(schema.fields().len(), 4, "got {schema:?}");
}

#[test]
fn wrapped_methods_advertise_the_canonical_envelope() {
    // Not every method is flat — `catalog_attach` genuinely carries a wrapped
    // request dataclass, and the generated table says so rather than inventing
    // fields.
    //
    // It does NOT equal `request_binary_schema()`, and that is the third thing
    // generation corrected: the hand-written helper declares `request` NULLABLE,
    // while the canonical Python protocol declares it non-null. These schemas are
    // advertisement-only — `params_schema_for` is consumed exclusively by
    // `protocol::register` to populate `__describe__`, never to validate an
    // inbound batch — so tightening the advertised nullability cannot reject
    // traffic that previously worked.
    //
    // `request_binary_schema()` is deliberately left lenient: it is now reached
    // only as the fallback for a method this build has never heard of, where
    // guessing non-null would be an overreach.
    let schema = params_schema_for("catalog_attach");
    assert_eq!(schema.fields().len(), 1, "got {schema:?}");
    let field = schema.field(0);
    assert_eq!(field.name(), "request");
    assert_eq!(field.data_type(), &DataType::Binary);
    assert!(
        !field.is_nullable(),
        "the canonical protocol declares the wrapped `request` column non-null",
    );
    assert!(
        request_binary_schema().field(0).is_nullable(),
        "the lenient fallback helper was supposed to stay nullable — did it change?",
    );
}
