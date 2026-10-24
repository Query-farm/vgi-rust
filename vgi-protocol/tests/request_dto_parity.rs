// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Hand-written request / response DTOs must match their generated schemas.
//!
//! These are the records a Rust client (`vgi-client`) serializes into a
//! request, and the records a worker writes back. Their shape — field
//! membership, order, types and nullability — is fixed by the canonical
//! Python `VgiProtocol`, transcribed into `generated::protocol_schemas`.
//! A client whose derived schema drifts writes a request a strict peer
//! rejects (a null in a non-nullable column, a missing optional column, or a
//! positional read off by one), so pin every one of them.

use vgi_protocol::generated::protocol_schemas as gen;
use vgi_protocol::protocol::dtos::*;
use vgi_protocol::wire::flat_schema;

macro_rules! parity {
    ($($test:ident: $dto:ty => $schema:ident),* $(,)?) => {$(
        #[test]
        fn $test() {
            assert_eq!(flat_schema::<$dto>(), gen::$schema());
        }
    )*};
}

parity! {
    copy_from_context: CopyFromContext => copy_from_context_schema,
    copy_to_context: CopyToContext => copy_to_context_schema,
    bind_request: BindRequest => bind_request_schema,
    init_request: InitRequest => init_request_schema,
    table_function_plan_request: TableFunctionPlanRequest => table_function_plan_request_schema,
    scan_split: ScanSplit => scan_split_schema,
    global_init_response: GlobalInitResponse => global_init_response_schema,
    catalog_attach_request: CatalogAttachRequest => catalog_attach_request_schema,
    client_capabilities: ClientCapabilities => client_capabilities_schema,
    attach_catalog_info: AttachCatalogInfo => attach_catalog_info_schema,
    foreign_key_info: ForeignKeyInfo => foreign_key_info_schema,
    index_info: IndexInfo => index_info_schema,
    copy_from_format_info: CopyFromFormatInfo => copy_from_format_info_schema,
    scan_branches_result: ScanBranchesResult => scan_branches_result_schema,
    table_buffering_process_request: TableBufferingProcessRequest => table_buffering_process_request_schema,
    table_buffering_combine_request: TableBufferingCombineRequest => table_buffering_combine_request_schema,
    table_buffering_destructor_request: TableBufferingDestructorRequest => table_buffering_destructor_request_schema,
    aggregate_bind_request: AggregateBindRequest => aggregate_bind_request_schema,
    aggregate_update_request: AggregateUpdateRequest => aggregate_update_request_schema,
    aggregate_combine_request: AggregateCombineRequest => aggregate_combine_request_schema,
    aggregate_finalize_request: AggregateFinalizeRequest => aggregate_finalize_request_schema,
    aggregate_destructor_request: AggregateDestructorRequest => aggregate_destructor_request_schema,
    aggregate_window_init_request: AggregateWindowInitRequest => aggregate_window_init_request_schema,
    aggregate_window_request: AggregateWindowRequest => aggregate_window_request_schema,
    aggregate_window_batch_request: AggregateWindowBatchRequest => aggregate_window_batch_request_schema,
    aggregate_window_destructor_request: AggregateWindowDestructorRequest => aggregate_window_destructor_request_schema,
    aggregate_streaming_open_request: AggregateStreamingOpenRequest => aggregate_streaming_open_request_schema,
    aggregate_streaming_chunk_request: AggregateStreamingChunkRequest => aggregate_streaming_chunk_request_schema,
    aggregate_streaming_close_request: AggregateStreamingCloseRequest => aggregate_streaming_close_request_schema,
    cardinality_request: CardinalityRequest => table_function_cardinality_request_schema,
    dynamic_to_string_request: DynamicToStringRequest => table_function_dynamic_to_string_request_schema,
    bind_response: BindResponse => bind_result_schema,
    plan_response: PlanResponse => table_function_plan_result_schema,
    cardinality_response: CardinalityResponse => table_function_cardinality_result_schema,
    dynamic_to_string_response: DynamicToStringResponse => table_function_dynamic_to_string_result_schema,
    table_buffering_process_response: TableBufferingProcessResponse => table_buffering_process_result_schema,
    table_buffering_combine_response: TableBufferingCombineResponse => table_buffering_combine_result_schema,
    aggregate_bind_response: AggregateBindResponse => aggregate_bind_result_schema,
    aggregate_finalize_response: AggregateFinalizeResponse => aggregate_finalize_result_schema,
    aggregate_window_response: AggregateWindowResponse => aggregate_window_result_schema,
    aggregate_streaming_open_response: AggregateStreamingOpenResponse => aggregate_streaming_open_result_schema,
    aggregate_streaming_chunk_response: AggregateStreamingChunkResponse => aggregate_streaming_chunk_result_schema,
    catalog_transaction_begin_result: CatalogTransactionBeginResult => catalog_transaction_begin_result_schema,
    catalog_version_result: CatalogVersionResult => catalog_version_result_schema,
}

/// The worker still decodes requests from peers that omit the optional
/// columns: `copy_from` / `copy_to` (the DuckDB extension sends them only for
/// COPY) and the non-nullable `filters_complete`, which defaults to `true`.
#[test]
fn backfills_restore_omitted_optional_columns() {
    use std::sync::Arc;

    use arrow_array::{Array, BinaryArray, BooleanArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let bind = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "function_name",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["f"]))],
    )
    .unwrap();
    let (bind, _) = backfill_bind_request(bind).unwrap();
    for name in ["copy_from", "copy_to", "argument_names", "schema_path"] {
        assert!(bind.column_by_name(name).unwrap().is_null(0), "{name}");
    }

    let plan = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "bind_call",
            DataType::Binary,
            false,
        )])),
        vec![Arc::new(BinaryArray::from(vec![&b""[..]]))],
    )
    .unwrap();
    let plan = backfill_plan_request(plan).unwrap();
    let complete = plan
        .column_by_name("filters_complete")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(complete.value(0) && complete.null_count() == 0);
}
