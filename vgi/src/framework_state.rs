// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Framework records kept in shared storage, and the process-local cache of
//! the buffered-function init record.
//!
//! A table-buffering function's `process` / `combine` RPCs carry neither the
//! bind call nor its schemas, and may land on any pooled worker process, so
//! the sink init persists that context once, as one [`BufferingInit`] record,
//! [sealed](crate::storage::seal) under [`BUFFERING_INIT_KEY`]. A process RPC
//! then needs exactly one storage read, and none at all once this process has
//! seen the execution: the record is immutable for the execution's life, so
//! [`BufferingInitCache`] keeps it (storage stays authoritative on a miss).
//!
//! The record encoding is length-prefixed and versioned. Decoding is strict:
//! a truncated, unknown or duplicated field is an error, never a default.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use vgi_rpc::{Result, RpcError};

use crate::ipc;
use crate::protocol::dtos::{ArgumentsLocation, CopyToContext};

/// Key/value key of the sealed [`BufferingInit`] record.
pub(crate) const BUFFERING_INIT_KEY: &[u8] = b"_vgi/buffering_init";
/// Key/value key of an aggregate's sealed bind-time arguments.
pub(crate) const AGGREGATE_ARGS_KEY: &[u8] = b"_vgi/aggregate_args";
/// Key/value key of a streaming aggregate's sealed session.
pub(crate) const STREAMING_SESSION_KEY: &[u8] = b"_vgi/streaming_session";

const INIT_MAGIC: &[u8; 4] = b"VBI1";
const SESSION_MAGIC: &[u8; 4] = b"VSA1";

// Field tags of a `BufferingInit` record.
const T_OUTPUT_SCHEMA: u8 = 1;
const T_INPUT_SCHEMA: u8 = 2;
const T_ARGUMENTS: u8 = 3;
const T_SECRETS: u8 = 4;
const T_FLAGS: u8 = 5;
const T_COPY_TO_FORMAT: u8 = 6;
const T_COPY_TO_PATH: u8 = 7;
const T_ATTACH: u8 = 8;
const T_SCHEMA_PATH: u8 = 9;
const T_ARGUMENTS_LOCATION: u8 = 10;

const FLAG_LOGGING: u8 = 0x01;

/// A bind's arguments as the client sent them: the bytes inline, or the
/// protocol 3.4 reference (large-values spec §6: anything that persists an
/// init request stores the reference, never a resolved argument).
#[derive(Debug, Clone)]
pub(crate) enum StoredArguments {
    /// The `Arguments` IPC stream.
    Inline(Vec<u8>),
    /// Where the uploaded arguments live; resolved again on read.
    Location(ArgumentsLocation),
}

impl PartialEq for StoredArguments {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Inline(a), Self::Inline(b)) => a == b,
            (Self::Location(a), Self::Location(b)) => {
                a.r#ref.0 == b.r#ref.0 && a.size == b.size && a.sha256 == b.sha256
            }
            _ => false,
        }
    }
}

/// Everything a buffered function's `process` / `combine` / finalize resume
/// needs from its bind, persisted once by the sink init.
#[derive(Debug, Clone)]
pub(crate) struct BufferingInit {
    /// The bound output schema (may differ from the input's).
    pub output_schema: SchemaRef,
    /// The bound input (source) schema.
    pub input_schema: Option<SchemaRef>,
    /// The call arguments as the client sent them.
    pub arguments: StoredArguments,
    /// The resolved secrets (`Secrets` IPC). The only field here that a
    /// process / combine request has no copy of at all, and the reason the
    /// record must be sealed.
    pub secrets: Option<Vec<u8>>,
    /// The `logging` named flag.
    pub logging: bool,
    /// The `COPY ... TO` destination.
    pub copy_to: Option<CopyToContext>,
    /// The attach scope, a fallback for a peer whose process / combine
    /// request omits `attach_opaque_data`.
    pub attach_opaque_data: Option<Vec<u8>>,
    /// The declaring schema, a fallback for a peer whose request omits
    /// `schema_path` (protocol < 1.2).
    pub schema_path: Option<Vec<String>>,
}

fn put_field(out: &mut Vec<u8>, tag: u8, value: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
}

fn corrupt(what: &str, why: impl std::fmt::Display) -> RpcError {
    RpcError::runtime_error(format!("corrupt {what} record in function storage: {why}"))
}

/// Split a tagged record body into `(tag, value)` fields, rejecting
/// truncation and duplicate tags.
fn fields<'a>(what: &str, magic: &[u8; 4], bytes: &'a [u8]) -> Result<HashMap<u8, &'a [u8]>> {
    let body = bytes
        .strip_prefix(magic.as_slice())
        .ok_or_else(|| corrupt(what, "unknown record format"))?;
    let mut out = HashMap::new();
    let mut pos = 0usize;
    while pos < body.len() {
        if body.len() - pos < 5 {
            return Err(corrupt(what, "truncated field header"));
        }
        let tag = body[pos];
        let len = u32::from_be_bytes(body[pos + 1..pos + 5].try_into().unwrap()) as usize;
        pos += 5;
        if body.len() - pos < len {
            return Err(corrupt(what, format!("field {tag} truncated")));
        }
        if out.insert(tag, &body[pos..pos + len]).is_some() {
            return Err(corrupt(what, format!("field {tag} repeated")));
        }
        pos += len;
    }
    Ok(out)
}

fn utf8(what: &str, field: &str, b: &[u8]) -> Result<String> {
    String::from_utf8(b.to_vec()).map_err(|_| corrupt(what, format!("{field} is not UTF-8")))
}

impl BufferingInit {
    /// The record's plaintext encoding (sealed before it is stored).
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = INIT_MAGIC.to_vec();
        put_field(
            &mut out,
            T_OUTPUT_SCHEMA,
            &ipc::write_schema_ref(&self.output_schema)?,
        );
        if let Some(s) = &self.input_schema {
            put_field(&mut out, T_INPUT_SCHEMA, &ipc::write_schema_ref(s)?);
        }
        match &self.arguments {
            StoredArguments::Inline(bytes) => put_field(&mut out, T_ARGUMENTS, bytes),
            StoredArguments::Location(location) => put_field(
                &mut out,
                T_ARGUMENTS_LOCATION,
                &ipc::write_batch(&crate::wire::to_batch(location.clone())?)?,
            ),
        }
        if let Some(s) = &self.secrets {
            put_field(&mut out, T_SECRETS, s);
        }
        put_field(
            &mut out,
            T_FLAGS,
            &[if self.logging { FLAG_LOGGING } else { 0 }],
        );
        if let Some(ct) = &self.copy_to {
            put_field(&mut out, T_COPY_TO_FORMAT, ct.format.as_bytes());
            put_field(&mut out, T_COPY_TO_PATH, ct.file_path.as_bytes());
        }
        if let Some(a) = &self.attach_opaque_data {
            put_field(&mut out, T_ATTACH, a);
        }
        if let Some(path) = &self.schema_path {
            let mut b = Vec::new();
            for part in path {
                b.extend_from_slice(&(part.len() as u32).to_be_bytes());
                b.extend_from_slice(part.as_bytes());
            }
            put_field(&mut out, T_SCHEMA_PATH, &b);
        }
        Ok(out)
    }

    /// Decode [`encode`](Self::encode)'s output; any malformation is an error.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        const WHAT: &str = "table-buffering init";
        let mut f = fields(WHAT, INIT_MAGIC, bytes)?;
        let mut take = |tag| f.remove(&tag);
        let output_schema = ipc::read_schema(
            take(T_OUTPUT_SCHEMA).ok_or_else(|| corrupt(WHAT, "no output schema"))?,
        )
        .map_err(|e| corrupt(WHAT, format!("output schema: {}", e.message)))?;
        let input_schema = take(T_INPUT_SCHEMA)
            .map(|b| {
                ipc::read_schema(b)
                    .map_err(|e| corrupt(WHAT, format!("input schema: {}", e.message)))
            })
            .transpose()?;
        let arguments = match (take(T_ARGUMENTS), take(T_ARGUMENTS_LOCATION)) {
            (Some(bytes), None) => StoredArguments::Inline(bytes.to_vec()),
            (None, Some(location)) => StoredArguments::Location(
                ipc::read_batch(location)
                    .and_then(|batch| crate::wire::from_batch(&batch))
                    .map_err(|e| corrupt(WHAT, format!("arguments location: {}", e.message)))?,
            ),
            (None, None) => return Err(corrupt(WHAT, "no arguments")),
            (Some(_), Some(_)) => {
                return Err(corrupt(WHAT, "both inline arguments and a location"))
            }
        };
        let secrets = take(T_SECRETS).map(<[u8]>::to_vec);
        let flags = take(T_FLAGS).ok_or_else(|| corrupt(WHAT, "no flags"))?;
        let [flags] = flags else {
            return Err(corrupt(WHAT, "flags are not one byte"));
        };
        if flags & !FLAG_LOGGING != 0 {
            return Err(corrupt(WHAT, format!("unknown flags {flags:#04x}")));
        }
        let copy_to = match (take(T_COPY_TO_FORMAT), take(T_COPY_TO_PATH)) {
            (None, None) => None,
            (Some(format), Some(path)) => Some(CopyToContext {
                format: utf8(WHAT, "COPY TO format", format)?,
                file_path: utf8(WHAT, "COPY TO path", path)?,
            }),
            _ => {
                return Err(corrupt(
                    WHAT,
                    "COPY TO format without path, or path without format",
                ))
            }
        };
        let attach_opaque_data = take(T_ATTACH).map(<[u8]>::to_vec);
        let schema_path = take(T_SCHEMA_PATH)
            .map(|mut b| {
                let mut parts = Vec::new();
                while !b.is_empty() {
                    if b.len() < 4 {
                        return Err(corrupt(WHAT, "schema path truncated"));
                    }
                    let n = u32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
                    if b.len() - 4 < n {
                        return Err(corrupt(WHAT, "schema path truncated"));
                    }
                    parts.push(utf8(WHAT, "schema path", &b[4..4 + n])?);
                    b = &b[4 + n..];
                }
                Ok(parts)
            })
            .transpose()?;
        if let Some(tag) = f.keys().next() {
            return Err(corrupt(WHAT, format!("unknown field {tag}")));
        }
        Ok(BufferingInit {
            output_schema,
            input_schema,
            arguments,
            secrets,
            logging: flags & FLAG_LOGGING != 0,
            copy_to,
            attach_opaque_data,
            schema_path,
        })
    }
}

/// A streaming aggregate's session, persisted by `aggregate_streaming_open`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamingSession {
    pub partition_key_count: i64,
    pub order_key_count: i64,
    /// The output schema (IPC), possibly empty.
    pub output_schema: Vec<u8>,
}

impl StreamingSession {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = SESSION_MAGIC.to_vec();
        out.extend_from_slice(&self.partition_key_count.to_be_bytes());
        out.extend_from_slice(&self.order_key_count.to_be_bytes());
        out.extend_from_slice(&self.output_schema);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        const WHAT: &str = "streaming aggregate session";
        let body = bytes
            .strip_prefix(SESSION_MAGIC.as_slice())
            .ok_or_else(|| corrupt(WHAT, "unknown record format"))?;
        if body.len() < 16 {
            return Err(corrupt(WHAT, "truncated"));
        }
        Ok(StreamingSession {
            partition_key_count: i64::from_be_bytes(body[..8].try_into().unwrap()),
            order_key_count: i64::from_be_bytes(body[8..16].try_into().unwrap()),
            output_schema: body[16..].to_vec(),
        })
    }
}

/// Upper bound on cached init records per process. An entry is a handful of
/// schemas and argument bytes; a worker rarely has more than a few buffered
/// executions live at once.
const INIT_CACHE_ENTRIES: usize = 256;

/// Process-local cache of decoded [`BufferingInit`] records, keyed by
/// execution id.
///
/// The `cloudflare-do` shard of a slot is a pure function of the execution id
/// ([`shard_key_for_scope`](crate::storage::shard_key_for_scope)) and every
/// other backend is unsharded, so the execution id alone identifies the slot.
/// Execution ids are 20 random bytes and never reused, and the record is
/// written once per execution, so an entry can never go stale; it is dropped
/// by the destructor (on the worker that runs it) or by FIFO eviction.
pub(crate) struct BufferingInitCache {
    inner: Mutex<CacheInner>,
    capacity: usize,
}

#[derive(Default)]
struct CacheInner {
    map: HashMap<Vec<u8>, Arc<BufferingInit>>,
    order: VecDeque<Vec<u8>>,
}

impl Default for BufferingInitCache {
    fn default() -> Self {
        Self::new(INIT_CACHE_ENTRIES)
    }
}

impl BufferingInitCache {
    pub fn new(capacity: usize) -> Self {
        BufferingInitCache {
            inner: Mutex::new(CacheInner::default()),
            capacity,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn get(&self, execution_id: &[u8]) -> Option<Arc<BufferingInit>> {
        self.lock().map.get(execution_id).cloned()
    }

    pub fn insert(&self, execution_id: &[u8], record: Arc<BufferingInit>) {
        if self.capacity == 0 {
            return;
        }
        let mut g = self.lock();
        if g.map.insert(execution_id.to_vec(), record).is_none() {
            g.order.push_back(execution_id.to_vec());
        }
        while g.map.len() > self.capacity {
            let Some(old) = g.order.pop_front() else {
                break;
            };
            g.map.remove(&old);
        }
    }

    pub fn remove(&self, execution_id: &[u8]) {
        let mut g = self.lock();
        if g.map.remove(execution_id).is_some() {
            g.order.retain(|k| k != execution_id);
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lock().map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn sample() -> BufferingInit {
        BufferingInit {
            output_schema: Arc::new(Schema::new(vec![Field::new("s", DataType::Float64, true)])),
            input_schema: Some(Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Int64,
                true,
            )]))),
            arguments: StoredArguments::Inline(b"args".to_vec()),
            secrets: Some(b"secrets".to_vec()),
            logging: true,
            copy_to: Some(CopyToContext {
                format: "fmt".into(),
                file_path: "/tmp/out".into(),
            }),
            attach_opaque_data: Some(b"attach".to_vec()),
            schema_path: Some(vec!["main".into(), "".into(), "x\0y".into()]),
        }
    }

    #[test]
    fn init_round_trips() {
        let r = sample();
        let d = BufferingInit::decode(&r.encode().unwrap()).unwrap();
        assert_eq!(d.output_schema, r.output_schema);
        assert_eq!(d.input_schema, r.input_schema);
        assert_eq!(d.arguments, r.arguments);
        assert_eq!(d.secrets, r.secrets);
        assert!(d.logging);
        assert_eq!(d.copy_to.as_ref().unwrap().file_path, "/tmp/out");
        assert_eq!(d.attach_opaque_data, r.attach_opaque_data);
        assert_eq!(d.schema_path, r.schema_path);

        let minimal = BufferingInit {
            input_schema: None,
            secrets: None,
            logging: false,
            copy_to: None,
            attach_opaque_data: None,
            schema_path: None,
            ..sample()
        };
        let d = BufferingInit::decode(&minimal.encode().unwrap()).unwrap();
        assert!(d.input_schema.is_none() && d.secrets.is_none() && d.copy_to.is_none());
        assert!(!d.logging);
    }

    /// An externalized argument is persisted as its reference, and a record
    /// with both forms (or neither) is corrupt.
    #[test]
    fn init_keeps_an_argument_reference() {
        let location = ArgumentsLocation {
            r#ref: vgi_rpc::Bytes(vec![7; 40]),
            size: 8 << 20,
            sha256: "0f".repeat(32),
        };
        let r = BufferingInit {
            arguments: StoredArguments::Location(location),
            ..sample()
        };
        let enc = r.encode().unwrap();
        let d = BufferingInit::decode(&enc).unwrap();
        assert_eq!(d.arguments, r.arguments);
        let mut both = enc.clone();
        put_field(&mut both, T_ARGUMENTS, b"args");
        assert!(BufferingInit::decode(&both).is_err());
    }

    #[test]
    fn malformed_init_is_an_error_at_every_truncation() {
        let enc = sample().encode().unwrap();
        // A prefix either fails, or ends exactly on a field boundary after
        // every required field (the optional trailing fields are absent):
        // never a half-read field silently defaulted.
        for n in 0..enc.len() {
            if let Ok(d) = BufferingInit::decode(&enc[..n]) {
                assert_eq!(d.encode().unwrap(), &enc[..n], "prefix {n}");
            }
        }
        let mut dup = enc.clone();
        put_field(&mut dup, T_ARGUMENTS, b"again");
        assert!(BufferingInit::decode(&dup).is_err());
        let mut unknown = enc.clone();
        put_field(&mut unknown, 99, b"");
        assert!(BufferingInit::decode(&unknown).is_err());
        let mut bad_magic = enc;
        bad_magic[0] = b'X';
        assert!(BufferingInit::decode(&bad_magic).is_err());
    }

    #[test]
    fn session_round_trips_and_rejects_truncation() {
        let s = StreamingSession {
            partition_key_count: 2,
            order_key_count: -1,
            output_schema: b"schema".to_vec(),
        };
        let enc = s.encode();
        assert_eq!(StreamingSession::decode(&enc).unwrap(), s);
        for n in 0..20 {
            assert!(StreamingSession::decode(&enc[..n]).is_err());
        }
    }

    #[test]
    fn cache_is_bounded_and_evicts() {
        let c = BufferingInitCache::new(2);
        let r = Arc::new(sample());
        c.insert(b"a", r.clone());
        c.insert(b"b", r.clone());
        c.insert(b"a", r.clone()); // re-insert does not grow the order
        c.insert(b"c", r.clone());
        assert_eq!(c.len(), 2);
        assert!(c.get(b"a").is_none(), "oldest evicted first");
        assert!(c.get(b"b").is_some() && c.get(b"c").is_some());
        c.remove(b"b");
        assert!(c.get(b"b").is_none());
        let off = BufferingInitCache::new(0);
        off.insert(b"a", r);
        assert!(off.get(b"a").is_none());
    }
}
