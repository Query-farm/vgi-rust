// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Execution-scoped diagnostics delivered alongside a worker's Arrow results.
//!
//! Keep one [`ExecutionDiagnostics`] in each producer, constructed from the
//! function name and [`ProcessParams::execution_id`](crate::ProcessParams::execution_id).
//! Logs use the existing RPC channel, so they reach clients even when subprocess
//! stderr is discarded. No process-global "last scan" state is involved.
//!
//! Counters describe one producer's source work, before SDK filtering. Persist
//! [`ScanStats`] and lifecycle flags with the producer's resume fields if it
//! supports HTTP continuation. A cancelled or failed scan may have no finish
//! event; errors must be returned as [`RpcError`], not only logged.

use std::io;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use vgi_rpc::error_model::{Code, ErrorDetail, ERROR_INFO_TYPE};
use vgi_rpc::{LogLevel, LogMessage, OutputCollector, RpcError};

/// Source work performed by one producer, before final filtering/projection.
///
/// These are cumulative counters, not deltas. A chunk is whatever read unit
/// the source uses (for example, a Zarr chunk or an Arrow IPC file). `bytes_read`
/// counts source bytes actually read, not decoded Arrow allocations. Leave a
/// counter at zero when no such work was performed; do not substitute estimates.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanStats {
    /// Source rows decoded, including rows subsequently rejected by filtering.
    pub rows_read: u64,
    /// Source chunks opened/read.
    pub chunks_read: u64,
    /// Source chunks avoided by conservative pruning.
    pub chunks_skipped: u64,
    /// Source bytes actually read.
    pub bytes_read: u64,
}

/// Correlation context for logs and errors from one function execution.
///
/// The `execution_id` extra is URL-safe base64 without padding. Encoding makes
/// arbitrary ID bytes printable; it is **not** encryption. Pass only the
/// execution ID, never opaque bind state, continuation tokens, or credentials.
/// This type intentionally has no timer, mutable counters, or automatic Drop
/// event: producers control when work actually starts and successfully ends.
#[derive(Debug, Clone)]
pub struct ExecutionDiagnostics {
    function: String,
    execution_id: String,
}

impl ExecutionDiagnostics {
    pub fn new(function: impl Into<String>, execution_id: &[u8]) -> Self {
        Self {
            function: function.into(),
            execution_id: URL_SAFE_NO_PAD.encode(execution_id),
        }
    }

    fn message(&self, level: LogLevel, event: &str, message: impl Into<String>) -> LogMessage {
        LogMessage::new(level, message)
            .with_extra("event", event)
            .with_extra("function", self.function.clone())
            .with_extra("execution_id", self.execution_id.clone())
    }

    /// Send a message with execution and function context through the RPC stream.
    /// Messages are client-visible; avoid source URLs, query text, and secrets.
    pub fn log(&self, out: &mut OutputCollector, level: LogLevel, message: impl Into<String>) {
        out.client_log_with(self.message(level, "worker.log", message));
    }

    /// Emit `scan.started` at the producer's first successful turn.
    /// Calling this does not remember whether a previous start was sent.
    pub fn scan_started(&self, out: &mut OutputCollector) {
        out.client_log_with(self.message(
            LogLevel::Info,
            "scan.started",
            format!("{}: source scan started", self.function),
        ));
    }

    /// Emit `scan.finished` with cumulative source counters on successful EOF.
    /// This is producer completion, not a transaction or whole-query success.
    pub fn scan_finished(&self, out: &mut OutputCollector, stats: &ScanStats) {
        out.client_log_with(
            // Keep totals readable in clients that display only the message;
            // structured consumers use the extras without parsing this text.
            self.message(
                LogLevel::Info,
                "scan.finished",
                format!(
                    "{}: source scan finished (rows_read={}, chunks_read={}, chunks_skipped={}, bytes_read={})",
                    self.function, stats.rows_read, stats.chunks_read, stats.chunks_skipped, stats.bytes_read,
                ),
            )
                .with_extra("rows_read", stats.rows_read.to_string())
                .with_extra("chunks_read", stats.chunks_read.to_string())
                .with_extra("chunks_skipped", stats.chunks_skipped.to_string())
                .with_extra("bytes_read", stats.bytes_read.to_string()),
        );
    }

    /// Add execution/function context while preserving the error's code,
    /// reason, and other typed details. Existing `ErrorInfo` is merged, avoiding
    /// duplicate detail types (which the RPC wire validator rejects).
    ///
    /// This does not redact an existing error message. Use [`source_io_error`]
    /// or [`source_http_error`] when the underlying error might include a URL
    /// containing credentials; those helpers never copy the raw error text.
    pub fn annotate_error(&self, error: RpcError) -> RpcError {
        with_error_context(
            error,
            [
                ("function", self.function.clone()),
                ("execution_id", self.execution_id.clone()),
            ],
        )
    }
}

/// Build a typed source error with safe, stable context.
///
/// `operation` and `source` are static labels such as `"read"` and
/// `"observations file"`, not the original path/URL or query. The message and
/// `ErrorInfo` contain only those labels and the canonical code. The stable
/// reason is `source_access_failed`; clients should branch on [`Code`].
pub fn source_error(code: Code, operation: &'static str, source: &'static str) -> RpcError {
    RpcError::runtime_error(format!("{operation} failed for {source} ({code})"))
        .with_status(code, "source_access_failed")
        .with_details([ErrorDetail::error_info([
            ("operation", operation),
            ("source", source),
        ])])
}

/// Classify a source I/O failure without copying potentially sensitive text.
///
/// A missing source and a permission failure remain distinct. Only the adapter
/// knows whether an absent object means fill values, no rows, or an error; this
/// helper always returns an error and never invents replacement data.
pub fn source_io_error(
    operation: &'static str,
    source: &'static str,
    error: &io::Error,
) -> RpcError {
    use io::ErrorKind;
    let code = match error.kind() {
        ErrorKind::NotFound => Code::NotFound,
        ErrorKind::PermissionDenied => Code::PermissionDenied,
        ErrorKind::TimedOut => Code::DeadlineExceeded,
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe
        | ErrorKind::WouldBlock => Code::Unavailable,
        ErrorKind::InvalidData | ErrorKind::UnexpectedEof => Code::DataLoss,
        ErrorKind::InvalidInput => Code::InvalidArgument,
        ErrorKind::AlreadyExists => Code::AlreadyExists,
        ErrorKind::Unsupported => Code::Unimplemented,
        _ => Code::Unknown,
    };
    let mapped = with_error_context(
        source_error(code, operation, source),
        [("io_kind", format!("{:?}", error.kind()))],
    );
    match error.raw_os_error() {
        Some(number) => with_error_context(mapped, [("os_error", number.to_string())]),
        None => mapped,
    }
}

/// Classify a failed HTTP source request without copying its URL, headers, or
/// response body. Call only after the source client identifies a failed request.
///
/// 403 is always `PermissionDenied`, never `NotFound`. Source-specific APIs
/// with different status semantics can call [`source_error`] directly instead.
pub fn source_http_error(operation: &'static str, source: &'static str, status: u16) -> RpcError {
    let code = match status {
        400 => Code::InvalidArgument,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 | 410 => Code::NotFound,
        408 | 504 => Code::DeadlineExceeded,
        416 => Code::OutOfRange,
        429 => Code::ResourceExhausted,
        500..=599 => Code::Unavailable,
        _ => Code::Unknown,
    };
    with_error_context(
        source_error(code, operation, source),
        [("http_status", status.to_string())],
    )
}

fn with_error_context<const N: usize>(
    mut error: RpcError,
    context: [(&str, String); N],
) -> RpcError {
    let mut details = error.error_details().to_vec();
    if let Some(detail) = details
        .iter_mut()
        .find(|detail| detail.get("@type").and_then(|v| v.as_str()) == Some(ERROR_INFO_TYPE))
    {
        // Helpers construct a valid ErrorInfo. For an arbitrary malformed raw
        // detail, leave its validation to RPC rather than creating a duplicate.
        if let Some(metadata) = detail.get_mut("metadata").and_then(|v| v.as_object_mut()) {
            for (key, value) in context {
                metadata.insert(key.to_string(), value.into());
            }
        }
    } else {
        details.push(ErrorDetail::error_info(context).to_json());
    }
    let code = error.error_code().to_string();
    error.set_wire_status(&code, details);
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_errors_preserve_classification_without_sensitive_text() {
        for (kind, expected) in [
            (io::ErrorKind::NotFound, Code::NotFound),
            (io::ErrorKind::PermissionDenied, Code::PermissionDenied),
            (io::ErrorKind::ConnectionReset, Code::Unavailable),
            (io::ErrorKind::TimedOut, Code::DeadlineExceeded),
            (io::ErrorKind::InvalidData, Code::DataLoss),
            (io::ErrorKind::Other, Code::Unknown),
        ] {
            let original = io::Error::new(kind, "https://user:password@source/data?token=secret");
            let error = source_io_error("read", "measurements", &original);
            assert_eq!(error.code(), expected);
            assert_eq!(error.error_info().unwrap()["source"], "measurements");
            assert_eq!(error.error_info().unwrap()["io_kind"], format!("{kind:?}"));
            assert!(!format!("{error:?}").contains("password"));
            assert!(!format!("{error:?}").contains("token="));
        }
    }

    #[test]
    fn http_codes_do_not_turn_denied_reads_into_missing_chunks() {
        for (status, code) in [
            (401, Code::Unauthenticated),
            (403, Code::PermissionDenied),
            (404, Code::NotFound),
            (429, Code::ResourceExhausted),
            (503, Code::Unavailable),
            (504, Code::DeadlineExceeded),
        ] {
            let error = source_http_error("read", "weather store", status);
            assert_eq!(error.code(), code);
            assert_eq!(
                error.error_info().unwrap()["http_status"],
                status.to_string()
            );
            assert_eq!(error.error_details().len(), 1);
        }
    }

    #[test]
    fn execution_context_merges_details_and_encodes_binary_ids() {
        let diagnostics = ExecutionDiagnostics::new("read_observations", &[0, 255, 254]);
        let error = source_http_error("read", "observations", 503)
            .with_details([ErrorDetail::retry_info(2.0)]);
        let error = diagnostics.annotate_error(diagnostics.annotate_error(error));
        assert_eq!(error.code(), Code::Unavailable);
        assert_eq!(error.error_kind(), "source_access_failed");
        assert_eq!(error.error_details().len(), 2);
        assert_eq!(error.retry_info(), Some(2.0));
        let info = error.error_info().unwrap();
        assert_eq!(info["function"], "read_observations");
        assert_eq!(info["execution_id"], "AP_-");
        assert_eq!(info["source"], "observations");
        assert_eq!(info["http_status"], "503");
    }
}
