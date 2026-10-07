// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Sealing `attach_opaque_data` and `transaction_opaque_data`.
//!
//! `catalog_attach` returns `attach_opaque_data` and
//! `catalog_transaction_begin` returns `transaction_opaque_data`; the client
//! stores them and sends them back on every later call. They are the worker's
//! own state held by the client, so on a transport that authenticates callers
//! (HTTP) the worker seals them and opens them again on the way in
//! (vgi-python `docs/protocol/vgi-opaque-data-sealing.md`, normative):
//!
//! - **Seal**: XChaCha20-Poly1305 (`vgi_rpc::crypto`) under the deployment's
//!   signing key -- `VGI_SIGNING_KEY` / [`crate::Worker::signing_key`], or 32
//!   random bytes generated when the HTTP server is built. Envelope version
//!   `0x02` for both values.
//! - **Bind to the caller**: AAD `"vgi.attach_opaque_data.v1" 0x00 ||
//!   identity`, identity `0x01 || domain || 0x00 || principal` for an
//!   authenticated caller, `0x00 || "anonymous"` otherwise.
//! - **Bind a transaction to its attach**: AAD
//!   `"vgi.transaction_opaque_data.v1" 0x00 || identity || 0x00 ||
//!   sealed attach`, the sealed attach exactly as the same call presents it.
//! - **Reject uniformly**: every failure -- wrong caller, wrong parent attach,
//!   tampered, malformed, unknown key, a plaintext value -- is the same error:
//!   message exactly `<field> not recognized`, code `INVALID_ARGUMENT`, kind
//!   `opaque_data_not_recognized`, no details. There is no plaintext fallback.
//!
//! The dispatcher never sees a sealed value: [`OpaqueSealer::open_batch`] runs
//! on every request before any handler, opening each value wherever the wire
//! carries one (top-level columns, the boxed `request`, a nested `bind_call`),
//! and [`OpaqueSealer::seal_result`] seals the two results that mint one. On
//! the OS-owned transports (stdio, unix, TCP launcher) there is no sealer and
//! values travel as the dispatcher produced them; secret attach options are
//! kept out of them regardless.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow_schema::DataType;
use vgi_rpc::{AuthContext, Result, RpcError};

use crate::ipc;

/// Envelope version byte of a sealed `attach_opaque_data`.
pub const ATTACH_ENVELOPE_VERSION: u8 = 2;
/// Envelope version byte of a sealed `transaction_opaque_data`.
pub const TRANSACTION_ENVELOPE_VERSION: u8 = 2;

const ATTACH_AAD_PREFIX: &[u8] = b"vgi.attach_opaque_data.v1\x00";
const TRANSACTION_AAD_PREFIX: &[u8] = b"vgi.transaction_opaque_data.v1\x00";

const ATTACH_FIELD: &str = "attach_opaque_data";
const TRANSACTION_FIELD: &str = "transaction_opaque_data";
/// Binary columns whose rows are nested IPC request batches that may carry
/// opaque values of their own.
const NESTED_FIELDS: [&str; 2] = ["request", "bind_call"];

/// The uniform rejection for an opaque value that does not open.
///
/// Identical for every failure mode, so a probing caller learns nothing about
/// which check failed; the two fields differ only in the field name.
pub fn opaque_data_rejected(field: &str) -> RpcError {
    RpcError::value_error(format!("{field} not recognized")).with_status(
        vgi_rpc::error_model::Code::InvalidArgument,
        OPAQUE_REJECTED_KIND,
    )
}

/// `error_kind` of [`opaque_data_rejected`] (code `INVALID_ARGUMENT`).
pub const OPAQUE_REJECTED_KIND: &str = "opaque_data_not_recognized";

/// A loggable stand-in for an opaque value: the first 12 hex characters of
/// SHA-256 over its lowercase hex text (vgi-python `vgi._redact.short_hash`,
/// vgi-rpc's `sentry.short_hash`). Never log the value itself, its hex, or a
/// hex prefix.
pub fn short_hash(value: &[u8]) -> String {
    use sha2::Digest as _;
    let hex: String = value.iter().map(|b| format!("{b:02x}")).collect();
    sha2::Sha256::digest(hex.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()[..12]
        .to_string()
}

/// The caller-identity part of both AADs.
fn identity_tail(auth: &AuthContext) -> Vec<u8> {
    if !auth.authenticated {
        return b"\x00anonymous".to_vec();
    }
    let mut out = vec![0x01];
    out.extend_from_slice(auth.domain.as_bytes());
    out.push(0);
    out.extend_from_slice(auth.principal.as_bytes());
    out
}

fn attach_aad(auth: &AuthContext) -> Vec<u8> {
    let mut aad = ATTACH_AAD_PREFIX.to_vec();
    aad.extend_from_slice(&identity_tail(auth));
    aad
}

fn transaction_aad(auth: &AuthContext, attach_envelope: &[u8]) -> Vec<u8> {
    let mut aad = TRANSACTION_AAD_PREFIX.to_vec();
    aad.extend_from_slice(&identity_tail(auth));
    aad.push(0);
    aad.extend_from_slice(attach_envelope);
    aad
}

/// Seals and opens opaque values under one signing key.
#[derive(Clone)]
pub struct OpaqueSealer {
    key: Arc<Vec<u8>>,
}

impl OpaqueSealer {
    /// A sealer over `key` (any length; normalized as every vgi-rpc envelope
    /// key is).
    pub fn new(key: Vec<u8>) -> Self {
        Self { key: Arc::new(key) }
    }

    /// Seal an attach plaintext for `auth`.
    pub fn seal_attach(&self, plaintext: &[u8], auth: &AuthContext) -> Vec<u8> {
        vgi_rpc::crypto::seal_bytes(
            plaintext,
            &self.key,
            &attach_aad(auth),
            ATTACH_ENVELOPE_VERSION,
        )
    }

    /// Open an attach envelope sealed for `auth`.
    pub fn open_attach(&self, envelope: &[u8], auth: &AuthContext) -> Result<Vec<u8>> {
        vgi_rpc::crypto::open_bytes(
            envelope,
            &self.key,
            &attach_aad(auth),
            ATTACH_ENVELOPE_VERSION,
        )
        .map_err(|_| opaque_data_rejected(ATTACH_FIELD))
    }

    /// Seal a transaction plaintext for `auth`, bound to `attach_envelope`
    /// (the sealed attach the call carried).
    pub fn seal_transaction(
        &self,
        plaintext: &[u8],
        auth: &AuthContext,
        attach_envelope: &[u8],
    ) -> Vec<u8> {
        vgi_rpc::crypto::seal_bytes(
            plaintext,
            &self.key,
            &transaction_aad(auth, attach_envelope),
            TRANSACTION_ENVELOPE_VERSION,
        )
    }

    /// Open a transaction envelope sealed for `auth` under `attach_envelope`.
    pub fn open_transaction(
        &self,
        envelope: &[u8],
        auth: &AuthContext,
        attach_envelope: &[u8],
    ) -> Result<Vec<u8>> {
        vgi_rpc::crypto::open_bytes(
            envelope,
            &self.key,
            &transaction_aad(auth, attach_envelope),
            TRANSACTION_ENVELOPE_VERSION,
        )
        .map_err(|_| opaque_data_rejected(TRANSACTION_FIELD))
    }

    /// Open every opaque value `batch` carries, at any depth the wire nests
    /// one, returning the batch the dispatcher sees. Unchanged (and not
    /// copied) when it carries none.
    ///
    /// A transaction value opens only beside the sealed attach of the same
    /// record; one presented without an attach is rejected.
    pub fn open_batch(&self, batch: &RecordBatch, auth: &AuthContext) -> Result<RecordBatch> {
        let schema = batch.schema();
        let touches = |name: &str| {
            name == ATTACH_FIELD || name == TRANSACTION_FIELD || NESTED_FIELDS.contains(&name)
        };
        if !schema.fields().iter().any(|f| touches(f.name())) {
            return Ok(batch.clone());
        }
        let sealed_attach = binary_column(batch, ATTACH_FIELD)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for (index, field) in schema.fields().iter().enumerate() {
            let column = batch.column(index);
            let name = field.name().as_str();
            let replaced = if name == ATTACH_FIELD {
                let values = sealed_attach.as_ref().expect("read above");
                map_binary(values, |_, sealed| self.open_attach(sealed, auth))?
            } else if name == TRANSACTION_FIELD {
                let values = binary_column(batch, TRANSACTION_FIELD)?.expect("present");
                map_binary(&values, |row, sealed| {
                    let attach = sealed_attach
                        .as_ref()
                        .filter(|a| a.is_valid(row))
                        .map(|a| a.value(row))
                        .ok_or_else(|| opaque_data_rejected(TRANSACTION_FIELD))?;
                    self.open_transaction(sealed, auth, attach)
                })?
            } else if NESTED_FIELDS.contains(&name) && is_binary(column.data_type()) {
                let values = binary_column(batch, name)?.expect("present");
                map_binary(&values, |_, nested| {
                    rewrite_nested(nested, |inner| self.open_batch(&inner, auth))
                })?
            } else {
                column.clone()
            };
            columns.push(replaced);
        }
        RecordBatch::try_new(schema, columns)
            .map_err(|e| RpcError::runtime_error(format!("rebuild opened request: {e}")))
    }

    /// Seal the opaque value a `catalog_attach` / `catalog_transaction_begin`
    /// result mints. `request` is the request as the caller sent it (sealed),
    /// whose attach a transaction binds to. Other methods pass through.
    pub fn seal_result(
        &self,
        method: &str,
        request: &RecordBatch,
        result: Option<RecordBatch>,
        auth: &AuthContext,
    ) -> Result<Option<RecordBatch>> {
        let field = match method {
            "catalog_attach" => ATTACH_FIELD,
            "catalog_transaction_begin" => TRANSACTION_FIELD,
            _ => return Ok(result),
        };
        let Some(envelope) = result else {
            return Ok(None);
        };
        let attach_envelope = if field == TRANSACTION_FIELD {
            let attach = binary_column(request, ATTACH_FIELD)?
                .filter(|a| !a.is_empty() && a.is_valid(0))
                .map(|a| a.value(0).to_vec())
                .ok_or_else(|| opaque_data_rejected(ATTACH_FIELD))?;
            Some(attach)
        } else {
            None
        };
        // `{result: binary}` around the IPC of the result record.
        let outer = binary_column(&envelope, "result")?
            .ok_or_else(|| RpcError::runtime_error("result envelope has no result column"))?;
        let sealed_outer = map_binary(&outer, |_, nested| {
            rewrite_nested(nested, |inner| {
                let Some(values) = binary_column(&inner, field)? else {
                    return Ok(inner);
                };
                let sealed = map_binary(&values, |_, plaintext| {
                    Ok(match &attach_envelope {
                        None => self.seal_attach(plaintext, auth),
                        Some(attach) => self.seal_transaction(plaintext, auth, attach),
                    })
                })?;
                let index = inner.schema().index_of(field).expect("present");
                let mut columns = inner.columns().to_vec();
                columns[index] = sealed;
                RecordBatch::try_new(inner.schema(), columns)
                    .map_err(|e| RpcError::runtime_error(format!("rebuild sealed result: {e}")))
            })
        })?;
        Ok(Some(
            RecordBatch::try_new(envelope.schema(), vec![sealed_outer])
                .map_err(|e| RpcError::runtime_error(format!("rebuild sealed result: {e}")))?,
        ))
    }
}

/// Decode a nested IPC record, transform it, and re-encode it under its
/// **original** declared schema: the reader relaxes nullability, and the
/// C++ client checks declared nullability on results it reads by position.
fn rewrite_nested(
    nested: &[u8],
    f: impl FnOnce(RecordBatch) -> Result<RecordBatch>,
) -> Result<Vec<u8>> {
    let declared = ipc::read_schema(nested)?;
    let out = f(ipc::read_batch(nested)?)?;
    ipc::write_batch_with_schema(&out, declared.as_ref())
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::LargeBinary)
}

/// Column `name` as a `BinaryArray` (`LargeBinary` cast down), or `None`
/// when absent. A non-binary column of an opaque name is not a value this
/// worker issued.
fn binary_column(batch: &RecordBatch, name: &str) -> Result<Option<BinaryArray>> {
    let Some(column) = batch.column_by_name(name) else {
        return Ok(None);
    };
    let field_error = || {
        if name == TRANSACTION_FIELD {
            opaque_data_rejected(TRANSACTION_FIELD)
        } else {
            opaque_data_rejected(ATTACH_FIELD)
        }
    };
    let binary = match column.data_type() {
        DataType::Binary => column.clone(),
        DataType::LargeBinary => {
            arrow_cast::cast(column, &DataType::Binary).map_err(|_| field_error())?
        }
        DataType::Null => return Ok(None),
        _ if name == ATTACH_FIELD || name == TRANSACTION_FIELD => return Err(field_error()),
        _ => return Ok(None),
    };
    Ok(binary.as_any().downcast_ref::<BinaryArray>().cloned())
}

/// Rebuild a binary column, mapping each non-null, non-empty value. Null and
/// empty values mean "no value" and pass through as they are.
fn map_binary(
    values: &BinaryArray,
    mut f: impl FnMut(usize, &[u8]) -> Result<Vec<u8>>,
) -> Result<ArrayRef> {
    let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            out.push(None);
        } else if values.value(row).is_empty() {
            out.push(Some(Vec::new()));
        } else {
            out.push(Some(f(row, values.value(row))?));
        }
    }
    Ok(Arc::new(BinaryArray::from_iter(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};

    fn alice() -> AuthContext {
        AuthContext::for_principal("bearer", "alice")
    }

    fn bob() -> AuthContext {
        AuthContext::for_principal("bearer", "bob")
    }

    fn sealer() -> OpaqueSealer {
        OpaqueSealer::new(b"test-signing-key".to_vec())
    }

    fn flat(attach: Option<&[u8]>, tx: Option<&[u8]>) -> RecordBatch {
        let mut fields = vec![Field::new(ATTACH_FIELD, DataType::Binary, true)];
        let mut cols: Vec<ArrayRef> = vec![Arc::new(BinaryArray::from(vec![attach]))];
        if tx.is_some() {
            fields.push(Field::new(TRANSACTION_FIELD, DataType::Binary, true));
            cols.push(Arc::new(BinaryArray::from(vec![tx])));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn opened(batch: &RecordBatch, name: &str) -> Vec<u8> {
        binary_column(batch, name)
            .unwrap()
            .unwrap()
            .value(0)
            .to_vec()
    }

    #[test]
    fn identity_tails_follow_the_reference() {
        assert_eq!(identity_tail(&alice()), b"\x01bearer\x00alice");
        assert_eq!(identity_tail(&AuthContext::anonymous()), b"\x00anonymous");
    }

    #[test]
    fn values_round_trip_for_their_owner() {
        let s = sealer();
        let attach = s.seal_attach(b"catalog-bytes", &alice());
        assert_eq!(attach[0], ATTACH_ENVELOPE_VERSION);
        let tx = s.seal_transaction(b"tx-bytes", &alice(), &attach);
        let out = s
            .open_batch(&flat(Some(&attach), Some(&tx)), &alice())
            .unwrap();
        assert_eq!(opened(&out, ATTACH_FIELD), b"catalog-bytes");
        assert_eq!(opened(&out, TRANSACTION_FIELD), b"tx-bytes");
    }

    /// Every failure mode, per field, is the one identical error.
    #[test]
    fn every_failure_is_the_same_uniform_rejection() {
        let s = sealer();
        let attach = s.seal_attach(b"catalog-bytes", &alice());
        let other_attach = s.seal_attach(b"catalog-bytes", &alice());
        let tx = s.seal_transaction(b"tx-bytes", &alice(), &attach);

        let mut attach_failures = Vec::new();
        // Replay as another principal; as anonymous; same principal, other domain.
        attach_failures.push(s.open_batch(&flat(Some(&attach), None), &bob()));
        attach_failures.push(s.open_batch(&flat(Some(&attach), None), &AuthContext::anonymous()));
        attach_failures.push(s.open_batch(
            &flat(Some(&attach), None),
            &AuthContext::for_principal("jwt", "alice"),
        ));
        // Tamper: first, middle and last byte.
        for at in [0, attach.len() / 2, attach.len() - 1] {
            let mut t = attach.clone();
            t[at] ^= 1;
            attach_failures.push(s.open_batch(&flat(Some(&t), None), &alice()));
        }
        // Plaintext of the old shapes, and another key.
        for forged in [
            b"example".to_vec(),
            [b"0123456789abcdef".as_slice(), b"\x00rest"].concat(),
            b"\x00sec\x00twin_a\x00vgi-exec-0000".to_vec(),
        ] {
            attach_failures.push(s.open_batch(&flat(Some(&forged), None), &alice()));
        }
        attach_failures.push(
            OpaqueSealer::new(b"another".to_vec()).open_batch(&flat(Some(&attach), None), &alice()),
        );

        let mut tx_failures = Vec::new();
        // A transaction replayed under another attach of the same catalog.
        tx_failures.push(s.open_batch(&flat(Some(&other_attach), Some(&tx)), &alice()));
        let tx_for_other = s.seal_transaction(b"tx-bytes", &alice(), &other_attach);
        tx_failures.push(s.open_batch(&flat(Some(&attach), Some(&tx_for_other)), &alice()));
        let mut t = tx.clone();
        t[tx.len() / 2] ^= 1;
        tx_failures.push(s.open_batch(&flat(Some(&attach), Some(&t)), &alice()));
        tx_failures.push(s.open_batch(&flat(Some(&attach), Some(b"tx-bytes")), &alice()));
        // A transaction without its attach.
        tx_failures.push(
            s.open_batch(
                &RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        TRANSACTION_FIELD,
                        DataType::Binary,
                        true,
                    )])),
                    vec![Arc::new(BinaryArray::from(vec![Some(tx.as_slice())]))],
                )
                .unwrap(),
                &alice(),
            ),
        );

        let render = |e: RpcError| {
            (
                e.error_type.clone(),
                e.message.clone(),
                e.error_kind.clone(),
                e.error_code().to_string(),
                e.error_details().to_vec(),
            )
        };
        let attach_errors: Vec<_> = attach_failures
            .into_iter()
            .map(|r| render(r.unwrap_err()))
            .collect();
        let tx_errors: Vec<_> = tx_failures
            .into_iter()
            .map(|r| render(r.unwrap_err()))
            .collect();
        assert!(attach_errors.iter().all(|e| e == &attach_errors[0]));
        assert!(tx_errors.iter().all(|e| e == &tx_errors[0]));
        assert_eq!(attach_errors[0].1, "attach_opaque_data not recognized");
        assert_eq!(attach_errors[0].2.as_deref(), Some(OPAQUE_REJECTED_KIND));
        assert_eq!(attach_errors[0].3, "INVALID_ARGUMENT");
        assert!(attach_errors[0].4.is_empty(), "no details");
        assert_eq!(tx_errors[0].1, "transaction_opaque_data not recognized");
        assert_eq!(attach_errors[0].0, tx_errors[0].0);
        assert_eq!(attach_errors[0].3, tx_errors[0].3);
    }

    #[test]
    fn nested_request_and_bind_call_are_opened() {
        let s = sealer();
        let attach = s.seal_attach(b"cat", &alice());
        let inner = flat(Some(&attach), None);
        let bind_call = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "bind_call",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from(vec![ipc::write_batch(&inner)
                .unwrap()
                .as_slice()]))],
        )
        .unwrap();
        let request = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "request",
                DataType::Binary,
                true,
            )])),
            vec![Arc::new(BinaryArray::from(vec![ipc::write_batch(
                &bind_call,
            )
            .unwrap()
            .as_slice()]))],
        )
        .unwrap();
        let out = s.open_batch(&request, &alice()).unwrap();
        let level1 = ipc::read_batch(&opened(&out, "request")).unwrap();
        let level2 = ipc::read_batch(&opened(&level1, "bind_call")).unwrap();
        assert_eq!(opened(&level2, ATTACH_FIELD), b"cat");
        assert!(s.open_batch(&request, &bob()).is_err());
    }

    #[test]
    fn short_hash_hashes_the_hex_text() {
        // sha256("00ff")[:12]
        assert_eq!(short_hash(&[0x00, 0xff]), "8909cde2f411");
        assert_eq!(short_hash(b"").len(), 12);
    }
}
