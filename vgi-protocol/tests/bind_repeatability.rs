// Copyright 2025, 2026 Query Farm LLC - https://query.farm
use arrow_array::types::Int16Type;
use arrow_array::{new_null_array, Array, DictionaryArray, Int16Array, StringArray};
use arrow_schema::Schema;
use std::sync::Arc;
use vgi_protocol::protocol::dtos::{BindRepeatability, BindResponse};
use vgi_protocol::{ipc, wire};
use vgi_rpc::{Bytes, DictString, VgiArrow};

#[test]
fn bind_repeatability_required_enum_round_trips() {
    assert_eq!(BindRepeatability::default(), BindRepeatability::Unknown);
    for value in [
        BindRepeatability::Unknown,
        BindRepeatability::StableWithinQuery,
    ] {
        let bound = BindResponse {
            output_schema: Bytes(ipc::write_schema(&Schema::empty()).unwrap()),
            opaque_data: None,
            lookup_secret_types: Vec::new(),
            lookup_scopes: Vec::new(),
            lookup_names: Vec::new(),
            projection_expression_profiles: Vec::new(),
            repeatability: value,
        };
        let row = wire::to_batch(bound).unwrap();
        assert_eq!(
            row.schema(),
            vgi_protocol::generated::protocol_schemas::bind_result_schema()
        );
        let bytes = ipc::write_batch(&row).unwrap();
        let decoded: BindResponse = wire::from_batch(&ipc::read_batch(&bytes).unwrap()).unwrap();
        assert_eq!(decoded.repeatability, value);
    }
}

#[test]
fn bind_repeatability_rejects_null_unknown_and_wrong_wire_type() {
    let null = new_null_array(&BindRepeatability::arrow_data_type(), 1);
    assert!(BindRepeatability::read(null.as_ref(), 0).is_err());
    for value in ["", "FUTURE", "stable_within_query"] {
        let column = DictString::build_singleton(DictString(value.into())).unwrap();
        assert!(BindRepeatability::read(column.as_ref(), 0).is_err());
    }
    let plain = StringArray::from(vec!["UNKNOWN"]);
    assert!(BindRepeatability::read(&plain, 0).is_err());
}

#[test]
fn bind_repeatability_rejects_hidden_dictionary_value_null() {
    let (offsets, bytes, _) = StringArray::from(vec!["UNKNOWN"]).into_parts();
    let null = StringArray::from(vec![None::<&str>]);
    let hidden = StringArray::new(offsets, bytes, null.nulls().cloned());
    assert!(hidden.is_null(0));
    assert_eq!(hidden.value(0), "UNKNOWN");
    let dictionary =
        DictionaryArray::<Int16Type>::try_new(Int16Array::from(vec![0]), Arc::new(hidden)).unwrap();
    assert!(!dictionary.is_null(0));
    assert!(BindRepeatability::read(&dictionary, 0).is_err());
}
