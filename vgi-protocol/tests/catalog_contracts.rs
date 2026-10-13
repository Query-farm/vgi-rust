// Copyright 2025, 2026 Query Farm LLC - https://query.farm

use std::io::Cursor;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use base64::Engine;
use vgi_protocol::generated::protocol_types::{OptionChanges, OptionState, RequiredFilterPaths};
use vgi_protocol::wire::{from_batch, to_batch};
use vgi_rpc::VgiArrow;

fn fixture(name: &str) -> Vec<u8> {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/catalog-contracts.json")).unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(corpus["records"][name].as_str().unwrap())
        .unwrap()
}

fn round_trip<T: VgiArrow>(name: &str) -> T {
    let mut source = StreamReader::try_new(Cursor::new(fixture(name)), None).unwrap();
    let value: T = from_batch(&source.next().unwrap().unwrap()).unwrap();
    assert!(source.next().is_none());
    let batch = to_batch(value).unwrap();
    let mut encoded = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut encoded, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let mut reader = StreamReader::try_new(Cursor::new(encoded), None).unwrap();
    from_batch(&reader.next().unwrap().unwrap()).unwrap()
}

#[test]
fn options_preserve_python_payloads_and_explicit_null_overrides() {
    let changes: OptionChanges = round_trip("changes");
    assert_eq!(changes.set_values.unwrap().0, fixture("values"));
    assert_eq!(changes.reset_names, ["reset.me"]);
    let reset: OptionChanges = round_trip("reset_only");
    assert!(reset.set_values.is_none());
    assert_eq!(reset.reset_names, ["reset.me"]);
    let state: OptionState = round_trip("state");
    assert_eq!(state.values.0, fixture("values"));
    assert_eq!(state.explicit_names, ["optional"]);
    let empty: OptionState = round_trip("empty_state");
    assert_eq!(empty.values.0, fixture("empty_values"));
    assert!(empty.explicit_names.is_empty());
}

#[test]
fn paths_preserve_literal_names_and_or_groups() {
    let paths: RequiredFilterPaths = round_trip("paths");
    assert_eq!(
        paths.groups,
        vec![
            vec![vec!["a.b"], vec!["a", "b"]],
            vec![vec!["record", "literal.\"name"]]
        ]
    );
    let empty: RequiredFilterPaths = round_trip("empty_paths");
    assert!(empty.groups.is_empty());
}
