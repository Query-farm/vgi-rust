// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `ticket_probe`: the cross-SDK fixture catalog for attach tickets.
//!
//! Every SDK's fixture worker serves this catalog identically, and the
//! extension's `attach_ticket/*.test` sqllogictests run against each of them
//! (vgi-python `docs/protocol/vgi-attach-tickets.md` §7; port of
//! `vgi/_test_fixtures/ticket_probe.py`):
//!
//! - catalog `ticket_probe`, default schema `main`;
//! - attach options, in this order: `region` (VARCHAR, default `'us-east-1'`)
//!   and `api_key` (VARCHAR, **required**, **secret**);
//! - table `main.probe`, backed by the table function `main.ticket_probe` (no
//!   arguments): one row, `region` and `api_key_sha256` -- the first 12
//!   lowercase hex characters of SHA-256(UTF-8(api_key)). The key itself is
//!   never returned.
//!
//! So a reattach with nothing but `vgi_attach_ticket` reading the same row
//! proves the secret option took effect without travelling again.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use sha2::Digest as _;
use vgi::arguments::Arguments;
use vgi::catalog::{AttachOptionFlags, CatSchema, CatTable, CatalogModel};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

/// The catalog's name.
pub const CATALOG_NAME: &str = "ticket_probe";
const FUNCTION_NAME: &str = "ticket_probe";
const DEFAULT_REGION: &str = "us-east-1";
const SEP: u8 = 0;

fn probe_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("api_key_sha256", DataType::Utf8, true),
    ]))
}

/// The first 12 lowercase hex characters of SHA-256(UTF-8(api_key)).
pub fn api_key_digest(api_key: &str) -> String {
    sha2::Sha256::digest(api_key.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()[..12]
        .to_string()
}

/// Row-0 string of the option named `name` (case-insensitively), if supplied.
fn option(options: Option<&RecordBatch>, name: &str) -> Option<String> {
    let batch = options?;
    let (index, _) = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name().eq_ignore_ascii_case(name))?;
    let col = arrow_cast::cast(batch.column(index), &DataType::Utf8).ok()?;
    let col = col.as_any().downcast_ref::<StringArray>()?;
    (batch.num_rows() > 0 && !col.is_null(0)).then(|| col.value(0).to_string())
}

/// The attach payload: `region \0 sha256(api_key)[:12]` -- never the key.
fn attach_payload(options: Option<&RecordBatch>) -> Result<Vec<u8>> {
    let region = option(options, "region")
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| DEFAULT_REGION.to_string());
    // The framework already refused an attach without the required option.
    let api_key = option(options, "api_key")
        .ok_or_else(|| RpcError::value_error("ticket_probe requires the api_key attach option"))?;
    let mut payload = region.into_bytes();
    payload.push(SEP);
    payload.extend_from_slice(api_key_digest(&api_key).as_bytes());
    Ok(payload)
}

/// `ticket_probe()` -- one row: the attached `region` and the key digest.
pub struct TicketProbeFunction;

impl TableFunction for TicketProbeFunction {
    fn name(&self) -> &str {
        FUNCTION_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Report the attach options of this ticket_probe attach (the api_key \
                          only as a digest)"
                .to_string(),
            categories: vec!["generator".into(), "testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: probe_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, _p: &BindParams) -> Option<TableCardinality> {
        Some(TableCardinality {
            estimate: Some(1),
            max: Some(1),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let payload = params
            .attach_opaque_data
            .as_deref()
            .and_then(vgi::catalog::secondary_attach_payload)
            .filter(|p| p.contains(&SEP))
            .ok_or_else(|| {
                RpcError::value_error(
                    "ticket_probe must be read through an attach of the ticket_probe catalog",
                )
            })?;
        let sep = payload.iter().position(|&b| b == SEP).unwrap();
        let region = String::from_utf8_lossy(&payload[..sep]).to_string();
        let digest = String::from_utf8_lossy(&payload[sep + 1..]).to_string();
        let full = RecordBatch::try_new(
            probe_schema(),
            vec![
                Arc::new(StringArray::from(vec![region])) as ArrayRef,
                Arc::new(StringArray::from(vec![digest])) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        // Honour projection: emit the columns bind's consumer asked for.
        let cols: Vec<ArrayRef> = params
            .output_schema
            .fields()
            .iter()
            .map(|f| {
                full.column_by_name(f.name())
                    .cloned()
                    .ok_or_else(|| RpcError::runtime_error(format!("no column {}", f.name())))
            })
            .collect::<Result<_>>()?;
        let batch = RecordBatch::try_new(params.output_schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(OneShot { batch: Some(batch) }))
    }
}

struct OneShot {
    batch: Option<RecordBatch>,
}

impl TableProducer for OneShot {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
    fn resume_supported(&self) -> bool {
        false
    }
}

/// The `ticket_probe` catalog model.
pub fn catalog() -> CatalogModel {
    let region_default: ArrayRef = Arc::new(StringArray::from(vec![DEFAULT_REGION]));
    let region = vgi::catalog::serialize_attach_option_spec_with_flags(
        "region",
        "Region the probe reports back",
        &DataType::Utf8,
        Some(&region_default),
        AttachOptionFlags::default(),
    )
    .expect("region spec");
    let api_key = vgi::catalog::serialize_attach_option_spec_with_flags(
        "api_key",
        "API key; only its digest is ever returned",
        &DataType::Utf8,
        None,
        AttachOptionFlags {
            required: true,
            secret: true,
        },
    )
    .expect("api_key spec");
    let probe = CatTable::new(
        "probe",
        probe_schema(),
        FUNCTION_NAME,
        Arguments::serialize_scan_args(&[]).unwrap_or_default(),
        Some("The options this attach was made with".to_string()),
        None,
    );
    CatalogModel {
        name: CATALOG_NAME.to_string(),
        comment: Some("Attach-ticket probe: one plain and one secret attach option".to_string()),
        attach_option_specs: vec![region, api_key],
        attach_payload: Some(Arc::new(attach_payload)),
        schemas: vec![CatSchema {
            path: Vec::new(),
            name: "main".to_string(),
            comment: None,
            tags: Vec::new(),
            views: Vec::new(),
            macros: Vec::new(),
            tables: vec![probe],
        }],
        catalog_version_frozen: true,
        ..Default::default()
    }
}

/// Register `main.ticket_probe` and the `ticket_probe` catalog.
pub fn register(w: &mut vgi::Worker) {
    w.register_table_in(CATALOG_NAME, "main", TicketProbeFunction);
    w.register_secondary_catalog(catalog(), vec![FUNCTION_NAME.to_string()]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_matches_the_spec_example() {
        assert_eq!(api_key_digest("sk-test-0123456789"), "0d3b56072291");
    }
}
