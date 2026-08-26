// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Exchange-mode calls: the client sends rows, the worker answers.
//!
//! Three shapes share this machinery, and they differ in what happens after
//! input ends:
//!
//! | Shape | Input phase | After input EOS |
//! |---|---|---|
//! | scalar | exchange, 1 batch in → 1 batch out | nothing |
//! | table-in-out | exchange, N in → M out | optional **FINALIZE** producer stream |
//! | buffered | unary `table_buffering_process` per chunk | `combine`, then a **TABLE_BUFFERING_FINALIZE** producer stream |
//!
//! The finalize phases are *producer* streams, not continuations of the
//! exchange: a fresh `init` carrying the phase and, crucially, **no
//! input_schema** — that absence is what puts the worker in tick mode rather
//! than exchange mode.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use vgi_protocol::cache_control::CacheControl;
use vgi_protocol::generated::request_params as p;
use vgi_protocol::protocol::dtos::{
    BindRequest, BindResponse, GlobalInitResponse, TableBufferingCombineRequest,
    TableBufferingCombineResponse, TableBufferingProcessRequest, TableBufferingProcessResponse,
};
use vgi_protocol::protocol::enums::phase;
use vgi_protocol::{ipc, wire};
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc::{Bytes, DictString};

use crate::catalog::AttachedCatalog;
use crate::client::VgiClient;
use crate::scan::{BindSpec, BoundFunction, Scan, ScanOptions};
use crate::transport::ExchangeStream;
use crate::wire_call::{call, envelope};

/// An open exchange: send input batches, read answers.
pub struct Exchange<'a> {
    stream: Box<dyn ExchangeStream + 'a>,
    header: GlobalInitResponse,
    schema: SchemaRef,
    parent_rows: Option<Vec<i32>>,
    last_cache_control: Option<CacheControl>,
    connection_reusable: Arc<AtomicBool>,
    closed: bool,
}

impl Exchange<'_> {
    /// The worker-minted id for this exchange.
    pub fn execution_id(&self) -> &Bytes {
        &self.header.execution_id
    }

    /// The output schema, as resolved at bind.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Per-output-row provenance from the most recent answer, if the worker
    /// supplied it.
    ///
    /// A `vgi_rpc.parent_row` array says, for each output row, which input row
    /// produced it — how a 1→N or 1→0 transform stays attributable. Its absence
    /// means an identity 1→1 map, in which case output rows correspond to input
    /// rows positionally.
    pub fn parent_rows(&self) -> Option<&[i32]> {
        self.parent_rows.as_deref()
    }

    /// Cache directives from the most recent advertising answer.
    ///
    /// Workers commonly advertise only on their first output batch, so the
    /// value is latched for the lifetime of the exchange rather than cleared
    /// when a later answer carries no `vgi.cache.*` metadata.
    pub fn cache_control(&self) -> Option<&CacheControl> {
        self.last_cache_control.as_ref()
    }

    /// Send one input batch and read the worker's answer.
    ///
    /// `Ok(None)` means the worker ended the stream. A zero-row answer is
    /// returned as an empty batch rather than `None`, because "no rows for this
    /// input" is a real answer in exchange mode and callers of a 1→0 transform
    /// need to see it.
    pub fn send(&mut self, input: &RecordBatch) -> Result<Option<RecordBatch>> {
        if self.closed {
            return Err(RpcError::protocol_error("send after close"));
        }
        let response = self.stream.exchange(input);
        if response.is_err() {
            self.connection_reusable.store(false, Ordering::Release);
        }
        match response? {
            None => Ok(None),
            Some((batch, md)) => {
                update_answer_metadata(&mut self.parent_rows, &mut self.last_cache_control, &md)?;
                Ok(Some(batch))
            }
        }
    }

    /// Signal input EOS.
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let result = self.stream.close();
        if result.is_err() {
            self.connection_reusable.store(false, Ordering::Release);
        }
        result
    }

    /// Ask the worker to stop early.
    pub fn cancel(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let result = self.stream.cancel();
        if result.is_err() {
            self.connection_reusable.store(false, Ordering::Release);
        }
        result
    }
}

impl Drop for Exchange<'_> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        // Dropping is often an early-return/error path, so there is nowhere to
        // report a cleanup failure. Mark closed first to preserve the same
        // exactly-once guarantee as explicit close/cancel, then best-effort
        // release the worker-side execution.
        self.closed = true;
        if self.stream.cancel().is_err() {
            self.connection_reusable.store(false, Ordering::Release);
        }
    }
}

/// Metadata key carrying per-output-row provenance.
const PARENT_ROW_KEY: &str = "vgi_rpc.parent_row#b64";

fn update_answer_metadata(
    parent_rows: &mut Option<Vec<i32>>,
    cache_control: &mut Option<CacheControl>,
    metadata: &vgi_rpc::wire::Metadata,
) -> Result<()> {
    *parent_rows = parse_parent_rows(metadata)?;
    if let Some(control) = CacheControl::from_metadata(metadata) {
        *cache_control = Some(control);
    }
    Ok(())
}

/// Decode the `vgi_rpc.parent_row` array: base64 of raw little-endian `int32`.
fn parse_parent_rows(md: &vgi_rpc::wire::Metadata) -> Result<Option<Vec<i32>>> {
    let Some(encoded) = md.get(PARENT_ROW_KEY) else {
        return Ok(None);
    };
    let raw = base64_decode(encoded)
        .ok_or_else(|| RpcError::type_error("vgi_rpc.parent_row is not valid base64"))?;
    if raw.len() % 4 != 0 {
        return Err(RpcError::type_error(format!(
            "vgi_rpc.parent_row is {} bytes, not a whole number of int32",
            raw.len()
        )));
    }
    Ok(Some(
        raw.chunks_exact(4)
            .map(|c| i32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect(),
    ))
}

/// Minimal standard-alphabet base64 decoder.
///
/// Hand-rolled to keep this crate's dependency list to Arrow and the RPC
/// client; the input is a short metadata value, so nothing here is hot.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(b: u8) -> Option<u32> {
        Some(match b {
            b'A'..=b'Z' => u32::from(b - b'A'),
            b'a'..=b'z' => u32::from(b - b'a') + 26,
            b'0'..=b'9' => u32::from(b - b'0') + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => return None,
        })
    }
    let bytes: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let body: &[u8] = match bytes.iter().position(|&b| b == b'=') {
        Some(i) => &bytes[..i],
        None => &bytes,
    };
    let mut out = Vec::with_capacity(body.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for &b in body {
        acc = (acc << 6) | val(b)?;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

impl VgiClient {
    /// Bind a function that takes input rows.
    ///
    /// The input schema is what makes this an exchange rather than a producer:
    /// the worker sees it on the bind and again on the init, and its presence is
    /// what selects exchange mode.
    pub fn bind_with_input(
        &mut self,
        cat: &AttachedCatalog,
        spec: &BindSpec,
        input_schema: &Schema,
    ) -> Result<BoundFunction> {
        self.bind_with_input_resolved(cat, spec, input_schema, None, false)
    }

    /// Bind an input function with the resolved secret batch requested by its
    /// first bind pass.
    pub fn bind_with_input_and_resolved_secrets(
        &mut self,
        cat: &AttachedCatalog,
        spec: &BindSpec,
        input_schema: &Schema,
        secrets: Vec<u8>,
    ) -> Result<BoundFunction> {
        self.bind_with_input_resolved(cat, spec, input_schema, Some(secrets), true)
    }

    fn bind_with_input_resolved(
        &mut self,
        cat: &AttachedCatalog,
        spec: &BindSpec,
        input_schema: &Schema,
        secrets: Option<Vec<u8>>,
        resolved_secrets_provided: bool,
    ) -> Result<BoundFunction> {
        let request = BindRequest {
            function_name: spec.function_name.clone(),
            arguments: spec.arguments.to_ipc()?,
            function_type: DictString(spec.function_type.as_str().to_string()),
            input_schema: Some(Bytes(ipc::write_schema(input_schema)?)),
            settings: spec.settings.clone(),
            secrets: secrets.map(Bytes),
            attach_opaque_data: Some(cat.handle().clone()),
            transaction_opaque_data: cat.transaction().cloned(),
            resolved_secrets_provided,
            at_unit: spec.at.as_ref().map(|a| a.unit.clone()),
            at_value: spec.at.as_ref().map(|a| a.value.clone()),
            schema_name: spec.schema_name.clone(),
        };
        let bind_call = envelope(request)?;
        let response: BindResponse = call(
            self.transport_mut(),
            "bind",
            p::BindParams {
                request: bind_call.clone(),
            },
        )?;
        let output_schema = if response.lookup_secret_types.is_empty() {
            ipc::read_schema(&response.output_schema.0).map_err(|e| {
                RpcError::type_error(format!("bind returned an unreadable output schema: {e}"))
            })?
        } else {
            Arc::new(Schema::empty())
        };
        Ok(BoundFunction::from_parts(
            spec.function_name.clone(),
            bind_call,
            response,
            output_schema,
        ))
    }

    /// Open an exchange over a bound input-taking function.
    pub fn open_exchange<'a>(
        &'a mut self,
        bound: &BoundFunction,
        opts: &ScanOptions,
    ) -> Result<Exchange<'a>> {
        let request = bound.init_request(opts, Some(phase::INPUT), None);
        let params = wire::to_batch(p::InitParams {
            request: envelope(request)?,
        })?;
        let connection_reusable = self.exchange_reuse_guard();
        let mut stream = match self.transport_mut().open_exchange("init", &params, true) {
            Ok(stream) => stream,
            Err(error) => {
                connection_reusable.store(false, Ordering::Release);
                return Err(error);
            }
        };
        let header = stream
            .header()
            .ok_or_else(|| RpcError::type_error("init did not return a GlobalInitResponse header"))
            .and_then(wire::from_batch);
        let header: GlobalInitResponse = match header {
            Ok(header) => header,
            Err(error) => {
                connection_reusable.store(false, Ordering::Release);
                let _ = stream.cancel();
                return Err(error);
            }
        };
        let schema = bound.output_schema().clone();
        Ok(Exchange {
            stream,
            header,
            schema,
            parent_rows: None,
            last_cache_control: None,
            connection_reusable,
            closed: false,
        })
    }

    /// Run the FINALIZE phase of a table-in-out function.
    ///
    /// This is a **producer** stream, not a continuation of the exchange: the
    /// init carries `phase = FINALIZE` and no input schema, which is what puts
    /// the worker back in tick mode.
    pub fn finalize_table_in_out<'a>(
        &'a mut self,
        bound: &BoundFunction,
        execution_id: &Bytes,
    ) -> Result<Scan<'a>> {
        let opts = ScanOptions {
            execution_id: Some(execution_id.clone()),
            ..Default::default()
        };
        self.open_phase_stream(bound, &opts, phase::FINALIZE, None)
    }

    /// Send one input chunk to a buffered function, returning the worker's
    /// opaque state id.
    ///
    /// Unlike the scan phases, the buffering RPCs re-resolve the function by
    /// `(schema, name)` rather than echoing the bind back — so they take the
    /// catalog handle and the function's coordinates directly.
    ///
    /// The state id bytes are chosen by the worker and round-tripped without
    /// inspection; the common pattern is for every chunk of one execution to
    /// answer with the same id so they land in one bucket.
    pub fn buffering_process(
        &mut self,
        cat: &AttachedCatalog,
        spec: &BindSpec,
        execution_id: &Bytes,
        input: &RecordBatch,
        batch_index: Option<i64>,
    ) -> Result<Bytes> {
        let request = TableBufferingProcessRequest {
            function_name: spec.function_name.clone(),
            execution_id: execution_id.clone(),
            input_batch: Bytes(ipc::write_batch(input)?),
            attach_opaque_data: Some(cat.handle().clone()),
            transaction_id: cat.transaction().cloned(),
            batch_index,
            schema_name: spec.schema_name.clone(),
        };
        let response: TableBufferingProcessResponse = call(
            self.transport_mut(),
            "table_buffering_process",
            p::TableBufferingProcessParams {
                request: envelope(request)?,
            },
        )?;
        Ok(response.state_id)
    }

    /// Collapse the per-chunk state ids into the ids the finalize phase drains.
    pub fn buffering_combine(
        &mut self,
        cat: &AttachedCatalog,
        spec: &BindSpec,
        execution_id: &Bytes,
        state_ids: Vec<Bytes>,
    ) -> Result<Vec<Bytes>> {
        let request = TableBufferingCombineRequest {
            function_name: spec.function_name.clone(),
            execution_id: execution_id.clone(),
            state_ids,
            attach_opaque_data: Some(cat.handle().clone()),
            transaction_id: cat.transaction().cloned(),
            schema_name: spec.schema_name.clone(),
        };
        let response: TableBufferingCombineResponse = call(
            self.transport_mut(),
            "table_buffering_combine",
            p::TableBufferingCombineParams {
                request: envelope(request)?,
            },
        )?;
        Ok(response.finalize_state_ids)
    }

    /// Drain one finalize state id of a buffered function.
    pub fn buffering_finalize<'a>(
        &'a mut self,
        bound: &BoundFunction,
        execution_id: &Bytes,
        finalize_state_id: &Bytes,
    ) -> Result<Scan<'a>> {
        let opts = ScanOptions {
            execution_id: Some(execution_id.clone()),
            ..Default::default()
        };
        self.open_phase_stream(
            bound,
            &opts,
            phase::TABLE_BUFFERING_FINALIZE,
            Some(finalize_state_id.clone()),
        )
    }

    /// Open the `TABLE_BUFFERING` init that mints an execution id.
    ///
    /// Runs once before any chunk is sent; peers reuse the id it returns.
    pub fn buffering_begin(&mut self, bound: &BoundFunction) -> Result<Bytes> {
        let opts = ScanOptions::default();
        let scan = self.open_phase_stream(bound, &opts, phase::TABLE_BUFFERING, None)?;
        Ok(scan.execution_id().clone())
    }

    fn open_phase_stream<'a>(
        &'a mut self,
        bound: &BoundFunction,
        opts: &ScanOptions,
        phase_name: &str,
        finalize_state_id: Option<Bytes>,
    ) -> Result<Scan<'a>> {
        let mut request = bound.init_request(opts, Some(phase_name), finalize_state_id);
        // A finalize is a producer stream. The worker selects tick mode by the
        // *absence* of an input schema, so it must not be echoed back here even
        // though the bind carried one.
        request.output_schema = bound.raw_output_schema().clone();
        let params = wire::to_batch(p::InitParams {
            request: envelope(request)?,
        })?;
        Scan::open(
            self.transport_mut(),
            &params,
            None,
            bound.function_name(),
            bound.output_schema().clone(),
        )
    }
}

/// Encode a `parent_row` array the way a worker would, for tests and for any
/// caller that needs to round-trip one.
#[cfg(test)]
pub(crate) fn encode_parent_rows(rows: &[i32]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut raw = Vec::with_capacity(rows.len() * 4);
    for r in rows {
        raw.extend_from_slice(&r.to_le_bytes());
    }
    let mut out = String::new();
    for chunk in raw.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use vgi_rpc::wire::Metadata;

    #[derive(Default)]
    struct StreamCalls {
        closes: AtomicUsize,
        cancels: AtomicUsize,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum StreamFailure {
        None,
        Send,
        Close,
        Cancel,
    }

    struct StubExchangeStream {
        calls: Arc<StreamCalls>,
        failure: StreamFailure,
    }

    impl ExchangeStream for StubExchangeStream {
        fn header(&self) -> Option<&RecordBatch> {
            None
        }

        fn exchange(&mut self, _input: &RecordBatch) -> Result<Option<(RecordBatch, Metadata)>> {
            if self.failure == StreamFailure::Send {
                return Err(RpcError::runtime_error("stub send failure"));
            }
            Ok(None)
        }

        fn close(&mut self) -> Result<()> {
            self.calls.closes.fetch_add(1, Ordering::SeqCst);
            if self.failure == StreamFailure::Close {
                return Err(RpcError::runtime_error("stub close failure"));
            }
            Ok(())
        }

        fn cancel(&mut self) -> Result<()> {
            self.calls.cancels.fetch_add(1, Ordering::SeqCst);
            if self.failure == StreamFailure::Cancel {
                return Err(RpcError::runtime_error("stub cancel failure"));
            }
            Ok(())
        }
    }

    fn stub_exchange(calls: Arc<StreamCalls>) -> Exchange<'static> {
        stub_exchange_with_failure(calls, StreamFailure::None).0
    }

    fn stub_exchange_with_failure(
        calls: Arc<StreamCalls>,
        failure: StreamFailure,
    ) -> (Exchange<'static>, Arc<AtomicBool>) {
        let connection_reusable = Arc::new(AtomicBool::new(true));
        let exchange = Exchange {
            stream: Box::new(StubExchangeStream { calls, failure }),
            header: GlobalInitResponse {
                execution_id: Bytes(b"stub".to_vec()),
                max_workers: 1,
                opaque_data: None,
            },
            schema: Arc::new(Schema::empty()),
            parent_rows: None,
            last_cache_control: None,
            connection_reusable: Arc::clone(&connection_reusable),
            closed: false,
        };
        (exchange, connection_reusable)
    }

    #[test]
    fn dropping_an_open_exchange_cancels_once() {
        let calls = Arc::new(StreamCalls::default());
        drop(stub_exchange(Arc::clone(&calls)));

        assert_eq!(calls.cancels.load(Ordering::SeqCst), 1);
        assert_eq!(calls.closes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn explicit_close_and_cancel_are_idempotent_and_suppress_drop_cleanup() {
        let closed_calls = Arc::new(StreamCalls::default());
        {
            let mut exchange = stub_exchange(Arc::clone(&closed_calls));
            exchange.close().unwrap();
            exchange.close().unwrap();
            exchange.cancel().unwrap();
        }
        assert_eq!(closed_calls.closes.load(Ordering::SeqCst), 1);
        assert_eq!(closed_calls.cancels.load(Ordering::SeqCst), 0);

        let cancelled_calls = Arc::new(StreamCalls::default());
        {
            let mut exchange = stub_exchange(Arc::clone(&cancelled_calls));
            exchange.cancel().unwrap();
            exchange.cancel().unwrap();
            exchange.close().unwrap();
        }
        assert_eq!(cancelled_calls.closes.load(Ordering::SeqCst), 0);
        assert_eq!(cancelled_calls.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn send_close_and_cancel_failures_make_the_connection_unpoolable() {
        let send_calls = Arc::new(StreamCalls::default());
        let (mut exchange, reusable) =
            stub_exchange_with_failure(Arc::clone(&send_calls), StreamFailure::Send);
        let input = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(exchange.send(&input).is_err());
        assert!(!reusable.load(Ordering::Acquire));
        drop(exchange);
        assert_eq!(send_calls.cancels.load(Ordering::SeqCst), 1);

        let close_calls = Arc::new(StreamCalls::default());
        let (mut exchange, reusable) =
            stub_exchange_with_failure(Arc::clone(&close_calls), StreamFailure::Close);
        assert!(exchange.close().is_err());
        assert!(!reusable.load(Ordering::Acquire));
        drop(exchange);
        assert_eq!(close_calls.closes.load(Ordering::SeqCst), 1);
        assert_eq!(close_calls.cancels.load(Ordering::SeqCst), 0);

        let cancel_calls = Arc::new(StreamCalls::default());
        let (mut exchange, reusable) =
            stub_exchange_with_failure(Arc::clone(&cancel_calls), StreamFailure::Cancel);
        assert!(exchange.cancel().is_err());
        assert!(!reusable.load(Ordering::Acquire));
        drop(exchange);
        assert_eq!(cancel_calls.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn absent_provenance_reads_as_an_identity_map() {
        assert!(parse_parent_rows(&Metadata::new()).unwrap().is_none());
    }

    #[test]
    fn provenance_round_trips() {
        for case in [
            vec![0i32],
            vec![0, 0, 1, 2, 2, 2],
            vec![7, 7, 7, 7],
            (0..100).collect::<Vec<i32>>(),
        ] {
            let mut md = Metadata::new();
            md.insert(PARENT_ROW_KEY.to_string(), encode_parent_rows(&case));
            assert_eq!(parse_parent_rows(&md).unwrap(), Some(case));
        }
    }

    #[test]
    fn a_truncated_provenance_array_is_rejected() {
        // Three bytes cannot be a whole number of int32 — better to fail than
        // to silently attribute rows to the wrong input.
        let mut md = Metadata::new();
        md.insert(PARENT_ROW_KEY.to_string(), "AAAA".to_string()); // 3 raw bytes
        assert!(parse_parent_rows(&md).is_err());
    }

    #[test]
    fn non_base64_provenance_is_rejected() {
        let mut md = Metadata::new();
        md.insert(PARENT_ROW_KEY.to_string(), "not base64!!".to_string());
        assert!(parse_parent_rows(&md).is_err());
    }

    #[test]
    fn cache_control_is_decoded_and_latched_across_answers() {
        let mut parent_rows = None;
        let mut cache_control = None;
        let first = Metadata::from([
            ("vgi.cache.ttl".to_string(), "300".to_string()),
            ("vgi.cache.scope".to_string(), "catalog".to_string()),
            ("vgi.cache.per_value".to_string(), "1".to_string()),
        ]);
        update_answer_metadata(&mut parent_rows, &mut cache_control, &first).unwrap();
        let control = cache_control.as_ref().expect("cache control");
        assert_eq!(control.ttl_seconds, Some(300));
        assert!(control.per_value);

        update_answer_metadata(&mut parent_rows, &mut cache_control, &Metadata::new()).unwrap();
        let control = cache_control.as_ref().expect("latched cache control");
        assert_eq!(control.ttl_seconds, Some(300));
        assert!(control.per_value);
    }
}
