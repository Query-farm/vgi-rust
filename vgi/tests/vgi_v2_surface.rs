// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! This port hosts the full `vgi.v2` surface, byte-identical to the reference.
//!
//! "The protocol is the unit of optionality": every SDK registers every
//! `vgi.v2` method with exactly the reference's method types and params,
//! result and header schemas, so `vgi_rpc.Reflection.v1` reports one protocol
//! hash everywhere. A method this port does not implement is still registered
//! and answers `UNIMPLEMENTED` / `method_not_implemented`.
//!
//! The hash covers names, method types and schemas (field names, order,
//! nullability, types) -- not docs or defaults. Any drift in a registration,
//! a DTO field order, or a nullability fails here.

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, RecordBatch};
use vgi::protocol::register::{not_implemented, UNIMPLEMENTED_METHODS};
use vgi::worker::Worker;
use vgi::VGI_PROTOCOL_NAME;
use vgi_rpc::metadata::{
    ERROR_CODE_KEY, ERROR_KIND_KEY, LOG_MESSAGE_KEY, PROTOCOL_KEY, PROTOCOL_VERSION_KEY,
    REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY,
};
use vgi_rpc::wire::{StreamReader, StreamWriter};

/// The reference `vgi.v2` protocol hash: vgi-python 0.43.0, 72 methods.
///
/// Changes only with `vgi.v2`'s protocol version (currently 2.1.0, see
/// `VGI_PROTOCOL_VERSION`). If this fails, this port's surface drifted from
/// the reference -- fix the registration, do not update the constant, unless
/// the protocol version itself moved.
const REFERENCE_VGI_V2_HASH: &str =
    "774cb80090d71ea76d09aa311b9cda4ca4c33c3bf72c43242eb6dc87b6f79ce5";

/// The reference's method count, for a readable failure before the hash.
const REFERENCE_VGI_V2_METHODS: usize = 72;

#[test]
fn vgi_v2_hash_equals_the_reference() {
    assert_eq!(vgi::VGI_PROTOCOL_VERSION, "2.1.0");
    let server = Worker::new().build_server();
    assert_eq!(server.protocol_name(), VGI_PROTOCOL_NAME);
    assert_eq!(
        server.methods().len(),
        REFERENCE_VGI_V2_METHODS,
        "method set differs from the reference: {:?}",
        server.sorted_method_names()
    );
    assert_eq!(server.protocol_hash(), REFERENCE_VGI_V2_HASH);
}

/// Every stubbed method answers every call with `UNIMPLEMENTED` /
/// `method_not_implemented` and the canonical message -- driven through the
/// real server dispatch, not the handler alone.
#[test]
fn unimplemented_methods_answer_unimplemented() {
    let server = Worker::new().build_server();
    for &method in UNIMPLEMENTED_METHODS {
        assert!(server.method(method).is_some(), "{method} not registered");

        let params = well_formed_params(method);

        let md = std::collections::HashMap::<String, String>::from([
            (RPC_METHOD_KEY.to_string(), method.to_string()),
            (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
            (PROTOCOL_KEY.to_string(), VGI_PROTOCOL_NAME.to_string()),
            (
                PROTOCOL_VERSION_KEY.to_string(),
                vgi::VGI_PROTOCOL_VERSION.to_string(),
            ),
        ]);
        let mut body = Vec::new();
        {
            let mut w = StreamWriter::new(&mut body, params.schema().as_ref()).unwrap();
            w.write(&params, Some(&md)).unwrap();
            w.finish().unwrap();
        }
        let mut out = Vec::new();
        server
            .serve_one(&mut std::io::Cursor::new(body), &mut out)
            .unwrap();

        let mut reader = StreamReader::new(std::io::Cursor::new(out)).unwrap();
        let mut error = None;
        while let Some((_, md)) = reader.read_next().unwrap() {
            if md.contains_key(ERROR_KIND_KEY) {
                error = Some(md);
                break;
            }
        }
        let md = error.unwrap_or_else(|| panic!("{method}: no error batch"));
        assert_eq!(
            md.get(ERROR_KIND_KEY).map(String::as_str),
            Some("method_not_implemented"),
            "{method}"
        );
        assert_eq!(
            md.get(ERROR_CODE_KEY).map(String::as_str),
            Some("UNIMPLEMENTED"),
            "{method}"
        );
        let expected = format!("{method} is not implemented by this worker");
        assert!(
            md.get(LOG_MESSAGE_KEY)
                .is_some_and(|m| m.contains(&expected)),
            "{method}: {md:?}"
        );
        assert_eq!(not_implemented(method).message, expected);
    }
}

/// A one-row, schema-valid params batch for `method`: every column non-null
/// (a placeholder value), every nullable column null.
fn well_formed_params(method: &str) -> RecordBatch {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::StringArray;
    use arrow_schema::DataType;
    let schema = vgi::wire::params_schema_for(method);
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| -> ArrayRef {
            if f.is_nullable() {
                return arrow_array::new_null_array(f.data_type(), 1);
            }
            match f.data_type() {
                DataType::Binary => Arc::new(BinaryArray::from(vec![b"x".as_slice()])),
                DataType::Utf8 => Arc::new(StringArray::from(vec!["t"])),
                DataType::List(item) if item.data_type() == &DataType::Utf8 => {
                    let mut b = ListBuilder::new(StringBuilder::new()).with_field(item.clone());
                    b.values().append_value("main");
                    b.append(true);
                    Arc::new(b.finish())
                }
                other => panic!("{method}: no placeholder for {other:?}"),
            }
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}
