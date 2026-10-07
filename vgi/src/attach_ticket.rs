// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Attach tickets: a user's ATTACH, sealed so a runner can replay it later as
//! that user (`vgi.attach_tickets.v1`).
//!
//! A ticket is the *what* half of an unattended session; a sealed grant
//! (`vgi_rpc.Identity.v1` `issue_grant`) is the *who*. While the user is
//! attached and logged in, a client asks the worker to seal the options it
//! attached with -- secret ones included -- into a ticket only this worker can
//! open ([`seal_attach`](ATTACH_TICKETS_PROTOCOL_NAME)). Later a runner holding
//! the user's grant attaches with the single option `vgi_attach_ticket`, and
//! the worker restores the sealed attach before any catalog code runs. The
//! runner never sees an option.
//!
//! A ticket carries no authority: it opens only under the *caller's*
//! principal, so without a grant (or login) for the same principal it attaches
//! nothing.
//!
//! Normative spec: vgi-python `docs/protocol/vgi-attach-tickets.md`. Byte-exact
//! vectors: `vgi/testdata/attach_ticket_vectors.json` (copied from vgi-python's
//! `vgi/_test_fixtures/attach_ticket_vectors.json`).
//!
//! ```text
//! ticket   = "vgia1." base64url_nopad( envelope )
//! envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(normalize(VGI_SIGNING_KEY), payload, aad)
//! aad      = "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)
//! payload  = issued_at i64le || expires_at i64le (0 = none)
//!            || ticket_id, catalog_name, data_version_spec, implementation_version  (u16le len || UTF-8 each)
//!            || options (u32le len || Arrow IPC of the one-row options record; <= 16 KiB)
//! ```
//!
//! The AAD binds the **principal only**, not `(domain, principal)`: a ticket is
//! sealed while the user is logged in (domain `jwt`, say) and opened when a
//! runner presents their grant (domain `grant`).

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use base64::Engine as _;
use vgi_protocol::protocol::dtos::CatalogAttachRequest;
use vgi_rpc::error_model::{Code, ErrorDetail, FieldViolation, PreconditionViolation};
use vgi_rpc::server::HostedProtocol;
use vgi_rpc::{AuthContext, Bytes, MethodInfo, Result, RpcError, VgiArrow};

use crate::dispatch::Dispatcher;
use crate::ipc;
use crate::wire;

/// Token prefix. The format version is in the prefix, so an incompatible
/// format is a different prefix -- never half-parsed.
pub const ATTACH_TICKET_PREFIX: &str = "vgia1.";

/// The reserved ATTACH option a runner presents a ticket in. No catalog may
/// declare an attach option with this name (compared case-insensitively).
pub const ATTACH_TICKET_OPTION: &str = crate::catalog::RESERVED_ATTACH_OPTION;

/// Wire name of the protocol hosting `seal_attach`.
pub const ATTACH_TICKETS_PROTOCOL_NAME: &str = "vgi.attach_tickets.v1";

/// Declared version of [`ATTACH_TICKETS_PROTOCOL_NAME`].
pub const ATTACH_TICKETS_PROTOCOL_VERSION: &str = "1.0.0";

/// The envelope's version byte, fixed by this format and independent of any
/// other envelope's.
pub const TICKET_ENVELOPE_VERSION: u8 = 0x01;

/// AAD domain. Distinct from the attach envelope's, so neither opens as the
/// other.
pub const TICKET_AAD_DOMAIN: &[u8] = b"vgi.attach_ticket.v1\x00";

/// Largest options record (serialized Arrow IPC bytes) a ticket may carry.
pub const MAX_OPTIONS_BYTES: usize = 16 * 1024;

/// Longest ticket text considered at all.
pub const MAX_TICKET_CHARS: usize = 32 * 1024;

/// Allowance for clocks disagreeing between the sealing and redeeming worker.
pub const CLOCK_SKEW_SECONDS: i64 = 60;

/// The environment variable holding the worker's signing key.
///
/// Only an explicitly configured key enables tickets: a key generated for one
/// process would make every ticket die on restart. This SDK never generates
/// one, so a key is either configured (this variable, or
/// [`Worker::signing_key`](crate::Worker::signing_key)) or absent.
pub const SIGNING_KEY_ENV: &str = "VGI_SIGNING_KEY";

const NONCE_LEN: usize = 24;
const MAX_TEXT: usize = 0xFFFF;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// `attach_ticket_invalid` / `INVALID_ARGUMENT`: one answer for every cause
/// (malformed, wrong key, wrong principal, tampered, bad payload), so a caller
/// cannot tell a forged ticket from another user's. Never contains the ticket.
pub fn attach_ticket_invalid(detail: &str) -> RpcError {
    RpcError::new("AttachTicketInvalidError", detail)
        .with_status(Code::InvalidArgument, "attach_ticket_invalid")
        .with_details([ErrorDetail::BadRequest {
            field_violations: vec![FieldViolation {
                field: ATTACH_TICKET_OPTION.to_string(),
                description: detail.to_string(),
            }],
        }])
}

/// `attach_ticket_expired` / `FAILED_PRECONDITION`: an authentic ticket
/// outside its lifetime. Only reported once the ticket opened under the
/// caller's principal.
pub fn attach_ticket_expired(detail: &str) -> RpcError {
    RpcError::new("AttachTicketExpiredError", detail)
        .with_status(Code::FailedPrecondition, "attach_ticket_expired")
        .with_details([ErrorDetail::PreconditionFailure {
            violations: vec![PreconditionViolation {
                r#type: "ATTACH_TICKET".to_string(),
                subject: ATTACH_TICKET_OPTION.to_string(),
                description: detail.to_string(),
            }],
        }])
}

/// `invalid_request` / `INVALID_ARGUMENT` with one `BadRequest` violation per
/// `(field, description)`.
fn invalid_request(message: &str, violations: Vec<(String, String)>) -> RpcError {
    RpcError::value_error(message)
        .with_status(Code::InvalidArgument, "invalid_request")
        .with_details([ErrorDetail::BadRequest {
            field_violations: violations
                .into_iter()
                .map(|(field, description)| FieldViolation { field, description })
                .collect(),
        }])
}

/// `action_denied` / `PERMISSION_DENIED` naming `seal_attach`.
fn action_denied(message: &str) -> RpcError {
    RpcError::permission_error(message)
        .with_status(Code::PermissionDenied, "action_denied")
        .with_details([ErrorDetail::error_info([("action", "seal_attach")])])
}

// ---------------------------------------------------------------------------
// Token format
// ---------------------------------------------------------------------------

/// What a ticket carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachTicketClaims {
    /// Seconds since the Unix epoch.
    pub issued_at: i64,
    /// Seconds since the Unix epoch; `0` means no expiry.
    pub expires_at: i64,
    /// 32 lowercase hex; a correlation handle, not a secret.
    pub ticket_id: String,
    /// The catalog the user attached. Non-empty.
    pub catalog_name: String,
    /// `""` when the user gave none.
    pub data_version_spec: String,
    /// `""` when the user gave none.
    pub implementation_version: String,
    /// Arrow IPC stream of the one-row options record, exactly as
    /// `CatalogAttachRequest.options` carries it; empty for none.
    pub options_ipc: Vec<u8>,
}

/// The ticket AAD: `"vgi.attach_ticket.v1" 0x00 || UTF-8(principal)`.
pub fn attach_ticket_aad(principal: &str) -> Vec<u8> {
    let mut aad = TICKET_AAD_DOMAIN.to_vec();
    aad.extend_from_slice(principal.as_bytes());
    aad
}

fn is_ticket_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn pack_text(out: &mut Vec<u8>, value: &str, field: &str) -> Result<()> {
    if value.len() > MAX_TEXT {
        return Err(RpcError::value_error(format!(
            "{field} is longer than 65535 bytes"
        )));
    }
    out.extend_from_slice(&(value.len() as u16).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_payload(claims: &AttachTicketClaims) -> Result<Vec<u8>> {
    if claims.options_ipc.len() > MAX_OPTIONS_BYTES {
        return Err(RpcError::value_error(format!(
            "options are {} bytes; a ticket carries at most {MAX_OPTIONS_BYTES}",
            claims.options_ipc.len()
        )));
    }
    let mut out = Vec::with_capacity(64 + claims.options_ipc.len());
    out.extend_from_slice(&claims.issued_at.to_le_bytes());
    out.extend_from_slice(&claims.expires_at.to_le_bytes());
    pack_text(&mut out, &claims.ticket_id, "ticket_id")?;
    pack_text(&mut out, &claims.catalog_name, "catalog_name")?;
    pack_text(&mut out, &claims.data_version_spec, "data_version_spec")?;
    pack_text(
        &mut out,
        &claims.implementation_version,
        "implementation_version",
    )?;
    out.extend_from_slice(&(claims.options_ipc.len() as u32).to_le_bytes());
    out.extend_from_slice(&claims.options_ipc);
    Ok(out)
}

/// Parse strictly: exact lengths, valid UTF-8, field rules, no trailing bytes.
fn decode_payload(payload: &[u8]) -> Result<AttachTicketClaims> {
    struct Reader<'a> {
        buf: &'a [u8],
        pos: usize,
    }
    impl<'a> Reader<'a> {
        fn take(&mut self, n: usize) -> Result<&'a [u8]> {
            if self.pos + n > self.buf.len() {
                return Err(attach_ticket_invalid("attach ticket payload is truncated"));
            }
            let chunk = &self.buf[self.pos..self.pos + n];
            self.pos += n;
            Ok(chunk)
        }
        fn i64(&mut self) -> Result<i64> {
            Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
        }
        fn text(&mut self) -> Result<String> {
            let len = u16::from_le_bytes(self.take(2)?.try_into().unwrap()) as usize;
            String::from_utf8(self.take(len)?.to_vec())
                .map_err(|_| attach_ticket_invalid("attach ticket payload is not UTF-8"))
        }
    }
    let mut r = Reader {
        buf: payload,
        pos: 0,
    };
    let issued_at = r.i64()?;
    let expires_at = r.i64()?;
    let ticket_id = r.text()?;
    let catalog_name = r.text()?;
    let data_version_spec = r.text()?;
    let implementation_version = r.text()?;
    let options_len = u32::from_le_bytes(r.take(4)?.try_into().unwrap()) as usize;
    if options_len > MAX_OPTIONS_BYTES {
        return Err(attach_ticket_invalid("attach ticket options exceed 16 KiB"));
    }
    let options_ipc = r.take(options_len)?.to_vec();
    if r.pos != payload.len() {
        return Err(attach_ticket_invalid(
            "attach ticket payload has trailing bytes",
        ));
    }
    if !is_ticket_id(&ticket_id) {
        return Err(attach_ticket_invalid(
            "attach ticket id is not 32 lowercase hex",
        ));
    }
    if catalog_name.is_empty() {
        return Err(attach_ticket_invalid("attach ticket names no catalog"));
    }
    if expires_at != 0 && expires_at <= issued_at {
        return Err(attach_ticket_invalid("attach ticket lifetime is empty"));
    }
    Ok(AttachTicketClaims {
        issued_at,
        expires_at,
        ticket_id,
        catalog_name,
        data_version_spec,
        implementation_version,
        options_ipc,
    })
}

fn b64url(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

/// Decode unpadded base64url, rejecting every spelling but the canonical one.
fn b64url_strict(text: &str) -> Result<Vec<u8>> {
    let alphabet_ok = !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if !alphabet_ok || text.len() % 4 == 1 {
        return Err(attach_ticket_invalid(
            "attach ticket is not unpadded base64url",
        ));
    }
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text)
        .or_else(|_| {
            // The strict engine refuses non-zero trailing bits outright; the
            // lenient decode plus re-encode below gives the same answer.
            base64::engine::GeneralPurpose::new(
                &base64::alphabet::URL_SAFE,
                base64::engine::GeneralPurposeConfig::new()
                    .with_encode_padding(false)
                    .with_decode_padding_mode(base64::engine::DecodePaddingMode::RequireNone)
                    .with_decode_allow_trailing_bits(true),
            )
            .decode(text)
        })
        .map_err(|_| attach_ticket_invalid("attach ticket is not unpadded base64url"))?;
    if b64url(&raw) != text {
        return Err(attach_ticket_invalid(
            "attach ticket is not canonical base64url",
        ));
    }
    Ok(raw)
}

/// Seal `claims` for `principal`, with a fresh random nonce.
///
/// `signing_key` is the worker's `VGI_SIGNING_KEY` bytes (any length;
/// normalized as `vgi_rpc::crypto` normalizes every envelope key).
///
/// # Errors
///
/// An empty principal or catalog, a malformed `ticket_id`, an empty lifetime,
/// oversize options, or a field too long to encode.
pub fn mint_attach_ticket(
    signing_key: &[u8],
    principal: &str,
    claims: &AttachTicketClaims,
) -> Result<String> {
    let mut nonce = [0u8; NONCE_LEN];
    getrandom_fill(&mut nonce);
    mint_with_nonce(signing_key, principal, claims, &nonce)
}

/// [`mint_attach_ticket`] with a fixed nonce: **test vectors only**.
fn mint_with_nonce(
    signing_key: &[u8],
    principal: &str,
    claims: &AttachTicketClaims,
    nonce: &[u8; NONCE_LEN],
) -> Result<String> {
    if principal.is_empty() {
        return Err(RpcError::value_error("a ticket needs a principal"));
    }
    if claims.catalog_name.is_empty() {
        return Err(RpcError::value_error("a ticket needs a catalog name"));
    }
    if !is_ticket_id(&claims.ticket_id) {
        return Err(RpcError::value_error("ticket_id must be 32 lowercase hex"));
    }
    if claims.expires_at != 0 && claims.expires_at <= claims.issued_at {
        return Err(RpcError::value_error(
            "expires_at must be 0 or after issued_at",
        ));
    }
    let envelope = vgi_rpc::crypto::seal_bytes_with_nonce(
        &encode_payload(claims)?,
        signing_key,
        &attach_ticket_aad(principal),
        TICKET_ENVELOPE_VERSION,
        nonce,
    );
    let token = format!("{ATTACH_TICKET_PREFIX}{}", b64url(&envelope));
    if token.len() > MAX_TICKET_CHARS {
        return Err(RpcError::value_error(format!(
            "the ticket would be {} characters; at most {MAX_TICKET_CHARS} are accepted",
            token.len()
        )));
    }
    Ok(token)
}

/// Fill `buf` from the thread-local CSPRNG the AEAD sealer itself uses.
fn getrandom_fill(buf: &mut [u8]) {
    use rand::RngCore as _;
    rand::thread_rng().fill_bytes(buf);
}

/// A fresh random `ticket_id`: 16 random bytes as 32 lowercase hex.
fn new_ticket_id() -> String {
    let mut raw = [0u8; 16];
    getrandom_fill(&mut raw);
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

/// Verify a ticket for the calling `principal` and return what it carries.
///
/// Order, normative: prefix, length, canonical base64url, caller, AEAD open
/// under the caller's principal, strict payload parse, then lifetime with a
/// 60 s skew. The lifetime is inside the ciphertext, so it is trusted only
/// after the tag verified.
///
/// # Errors
///
/// [`attach_ticket_invalid`] for every cause but the lifetime;
/// [`attach_ticket_expired`] for an authentic ticket outside it.
pub fn open_attach_ticket(
    signing_key: &[u8],
    token: &str,
    principal: Option<&str>,
    now: f64,
) -> Result<AttachTicketClaims> {
    let Some(body) = token.strip_prefix(ATTACH_TICKET_PREFIX) else {
        return Err(attach_ticket_invalid("not an attach ticket"));
    };
    if token.len() > MAX_TICKET_CHARS {
        return Err(attach_ticket_invalid("attach ticket is too long"));
    }
    let envelope = b64url_strict(body)?;
    let principal = match principal {
        Some(p) if !p.is_empty() => p,
        _ => {
            return Err(attach_ticket_invalid(
                "an anonymous caller cannot redeem an attach ticket",
            ))
        }
    };
    let payload = vgi_rpc::crypto::open_bytes(
        &envelope,
        signing_key,
        &attach_ticket_aad(principal),
        TICKET_ENVELOPE_VERSION,
    )
    .map_err(|_| attach_ticket_invalid("attach ticket failed verification"))?;
    let claims = decode_payload(&payload)?;
    if claims.issued_at as f64 > now + CLOCK_SKEW_SECONDS as f64 {
        return Err(attach_ticket_expired("attach ticket is not yet valid"));
    }
    if claims.expires_at != 0 && now >= (claims.expires_at + CLOCK_SKEW_SECONDS) as f64 {
        return Err(attach_ticket_expired("attach ticket has expired"));
    }
    Ok(claims)
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Redemption: what catalog_attach does with `vgi_attach_ticket`
// ---------------------------------------------------------------------------

fn caller_principal(auth: &AuthContext) -> Option<&str> {
    (auth.authenticated && !auth.principal.is_empty()).then_some(auth.principal.as_str())
}

/// Read row 0 of a string-like column, or `None` for a non-string or null.
fn string_value(col: &ArrayRef) -> Option<String> {
    let as_utf8 = match col.data_type() {
        DataType::Utf8 => col.clone(),
        DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
            arrow_cast::cast(col, &DataType::Utf8).ok()?
        }
        _ => return None,
    };
    let s = as_utf8.as_any().downcast_ref::<StringArray>()?;
    (!s.is_empty() && !s.is_null(0)).then(|| s.value(0).to_string())
}

/// Replace a ticket-carrying `catalog_attach` request with the attach it seals.
///
/// Returns `Ok(None)` when the options carry no `vgi_attach_ticket` (the
/// request is untouched). Otherwise the request the user originally made: the
/// sealed catalog name, options and version specs, with this request's
/// `client_capabilities`. The incoming `name` is ignored.
///
/// # Errors
///
/// `invalid_request` when any other option rides alongside the ticket (checked
/// before the ticket is opened: the sealed options are authoritative, so there
/// is nothing to merge); [`attach_ticket_invalid`] / [`attach_ticket_expired`]
/// from [`open_attach_ticket`], for a non-string value, or for a worker with no
/// signing key (every transport but a keyed HTTP one).
pub fn redeem_attach_ticket(
    request: &CatalogAttachRequest,
    signing_key: Option<&[u8]>,
    auth: &AuthContext,
    now: f64,
) -> Result<Option<CatalogAttachRequest>> {
    let Some(options_ipc) = request.options.as_ref().filter(|b| !b.0.is_empty()) else {
        return Ok(None);
    };
    // Unreadable options cannot carry a ticket; the ordinary attach reports
    // them on its own terms.
    let Ok(batch) = ipc::read_batch(&options_ipc.0) else {
        return Ok(None);
    };
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let Some(ticket_index) = names
        .iter()
        .position(|n| n.eq_ignore_ascii_case(ATTACH_TICKET_OPTION))
    else {
        return Ok(None);
    };
    let others: Vec<(String, String)> = names
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != ticket_index)
        .map(|(_, n)| {
            (
                format!("options.{n}"),
                format!("not allowed alongside {ATTACH_TICKET_OPTION}"),
            )
        })
        .collect();
    if !others.is_empty() {
        return Err(invalid_request(
            &format!("{ATTACH_TICKET_OPTION} must be the only attach option"),
            others,
        ));
    }
    let token = (batch.num_rows() == 1)
        .then(|| string_value(batch.column(ticket_index)))
        .flatten()
        .ok_or_else(|| {
            attach_ticket_invalid(&format!("{ATTACH_TICKET_OPTION} must be a string"))
        })?;
    let Some(key) = signing_key else {
        return Err(attach_ticket_invalid(
            "this worker does not redeem attach tickets",
        ));
    };
    let claims = open_attach_ticket(key, &token, caller_principal(auth), now)?;
    if !claims.options_ipc.is_empty() && ipc::read_batch(&claims.options_ipc).is_err() {
        return Err(attach_ticket_invalid(
            "attach ticket options are not an Arrow IPC record",
        ));
    }
    Ok(Some(CatalogAttachRequest {
        name: claims.catalog_name,
        options: (!claims.options_ipc.is_empty()).then_some(Bytes(claims.options_ipc)),
        data_version_spec: (!claims.data_version_spec.is_empty())
            .then_some(claims.data_version_spec),
        implementation_version: (!claims.implementation_version.is_empty())
            .then_some(claims.implementation_version),
        client_capabilities: request.client_capabilities.clone(),
    }))
}

// ---------------------------------------------------------------------------
// vgi.attach_tickets.v1
// ---------------------------------------------------------------------------

/// `SealAttachRequest`, IPC-serialized inside `seal_attach`'s `request`.
#[derive(Debug, Clone, VgiArrow)]
pub struct SealAttachRequest {
    /// The catalog the caller attached.
    pub catalog_name: String,
    /// Arrow IPC of the one-row options record, as
    /// `CatalogAttachRequest.options`; `None` for none.
    pub options: Option<Bytes>,
    /// As given at ATTACH; `""` for none.
    pub data_version_spec: String,
    /// As given at ATTACH; `""` for none.
    pub implementation_version: String,
    /// Requested lifetime; `0` asks for as long as the worker allows.
    pub ttl_seconds: i64,
}

/// `AttachTicket`, IPC-serialized inside `seal_attach`'s `result`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AttachTicket {
    /// The `vgia1.` text. Not a credential, but never logged.
    pub ticket: String,
    /// Unix seconds after which the worker refuses it; `+inf` for no expiry.
    pub expires_at: f64,
}

/// `seal_attach`'s params: one non-null `request` binary column.
fn seal_attach_params_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "request",
        DataType::Binary,
        false,
    )]))
}

/// The ticket lifetime ceiling: the grant keys' maximum when configured,
/// otherwise `VGI_RPC_GRANT_MAX_TTL_SECONDS` when set, otherwise none.
///
/// # Errors
///
/// The environment value is not a positive integer.
pub fn resolve_ticket_max_ttl(
    grant_keys: Option<&vgi_rpc::grants::GrantKeys>,
) -> std::result::Result<Option<i64>, String> {
    if let Some(keys) = grant_keys {
        return Ok(Some(keys.max_ttl_seconds()));
    }
    let raw = std::env::var(vgi_rpc::grants::GRANT_MAX_TTL_ENV).unwrap_or_default();
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    match raw.parse::<i64>() {
        Ok(v) if v > 0 => Ok(Some(v)),
        _ => Err(format!(
            "{}={raw:?} must be a positive integer",
            vgi_rpc::grants::GRANT_MAX_TTL_ENV
        )),
    }
}

/// Decoded `(name, required)` of each attach option a catalog declares.
fn declared_options(specs: &[Vec<u8>]) -> Vec<(String, bool)> {
    let required = crate::catalog::required_attach_option_names(specs);
    crate::catalog::attach_option_names(specs)
        .into_iter()
        .map(|n| {
            let r = required.contains(&n);
            (n, r)
        })
        .collect()
}

/// Validate and seal one `seal_attach` request (spec §5.3).
pub(crate) fn seal_attach(
    disp: &Dispatcher,
    signing_key: &[u8],
    max_ttl_seconds: Option<i64>,
    request: SealAttachRequest,
    auth: &AuthContext,
    now: i64,
) -> Result<AttachTicket> {
    let Some(principal) = caller_principal(auth) else {
        return Err(action_denied(
            "an anonymous caller cannot seal an attach ticket",
        ));
    };
    let mut violations: Vec<(String, String)> = Vec::new();
    if request.ttl_seconds < 0 {
        violations.push((
            "ttl_seconds".into(),
            "must be 0 (as long as allowed) or positive".into(),
        ));
    }
    let batch: Option<RecordBatch> = match request.options.as_ref().filter(|b| !b.0.is_empty()) {
        Some(bytes) => Some(ipc::read_batch(&bytes.0).map_err(|_| {
            invalid_request(
                "seal_attach request is invalid",
                vec![("options".into(), "not an Arrow IPC record".into())],
            )
        })?),
        None => None,
    };
    let mut names: Vec<String> = Vec::new();
    if let Some(b) = &batch {
        if b.num_rows() > 1 {
            violations.push(("options".into(), "must be a one-row record".into()));
        } else if b.num_rows() == 1 {
            names = b
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
        }
    }
    match disp.attach_option_specs_of(&request.catalog_name) {
        None => violations.push((
            "catalog_name".into(),
            format!("no catalog named {:?}", request.catalog_name),
        )),
        Some(specs) => {
            let declared = declared_options(&specs);
            for name in &names {
                if name.eq_ignore_ascii_case(ATTACH_TICKET_OPTION) {
                    violations.push((
                        format!("options.{name}"),
                        "a ticket cannot seal another ticket".into(),
                    ));
                } else if !declared.iter().any(|(d, _)| d.eq_ignore_ascii_case(name)) {
                    violations.push((
                        format!("options.{name}"),
                        "not an attach option this catalog declares".into(),
                    ));
                }
            }
            for (spec, required) in &declared {
                if *required && !names.iter().any(|n| n.eq_ignore_ascii_case(spec)) {
                    violations.push((format!("options.{spec}"), "required".into()));
                }
            }
        }
    }
    let options_ipc = match (&request.options, names.is_empty()) {
        (Some(bytes), false) => bytes.0.clone(),
        _ => Vec::new(),
    };
    if options_ipc.len() > MAX_OPTIONS_BYTES {
        violations.push((
            "options".into(),
            format!(
                "{} bytes; a ticket carries at most {MAX_OPTIONS_BYTES}",
                options_ipc.len()
            ),
        ));
    }
    if !violations.is_empty() {
        return Err(invalid_request(
            "seal_attach request is invalid",
            violations,
        ));
    }
    // 0 asks for the ceiling; otherwise the request, capped at the ceiling.
    let lifetime = match (request.ttl_seconds, max_ttl_seconds) {
        (0, ceiling) => ceiling,
        (ttl, None) => Some(ttl),
        (ttl, Some(ceiling)) => Some(ttl.min(ceiling)),
    };
    let expires_at = lifetime.map_or(0, |l| now + l);
    let claims = AttachTicketClaims {
        issued_at: now,
        expires_at,
        ticket_id: new_ticket_id(),
        catalog_name: request.catalog_name,
        data_version_spec: request.data_version_spec,
        implementation_version: request.implementation_version,
        options_ipc,
    };
    let ticket = mint_attach_ticket(signing_key, principal, &claims).map_err(|e| {
        invalid_request(
            "seal_attach request is invalid",
            vec![("request".into(), e.message)],
        )
    })?;
    Ok(AttachTicket {
        ticket,
        expires_at: if expires_at == 0 {
            f64::INFINITY
        } else {
            expires_at as f64
        },
    })
}

/// Build `vgi.attach_tickets.v1` over `disp`, sealing with `signing_key`.
pub(crate) fn attach_tickets_protocol(
    disp: Arc<Dispatcher>,
    signing_key: Vec<u8>,
    max_ttl_seconds: Option<i64>,
) -> HostedProtocol {
    HostedProtocol::new(ATTACH_TICKETS_PROTOCOL_NAME)
        .with_version(ATTACH_TICKETS_PROTOCOL_VERSION)
        .with_method(MethodInfo::unary(
            "seal_attach",
            seal_attach_params_schema(),
            wire::result_binary_schema(),
            move |req, ctx| {
                let col = req
                    .column("request")
                    .ok_or_else(|| RpcError::type_error("request missing 'request' column"))?;
                let bytes = col
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .filter(|b| !b.is_empty() && !b.is_null(0))
                    .ok_or_else(|| RpcError::type_error("'request' must be a non-null binary"))?;
                let request: SealAttachRequest =
                    wire::from_batch(&ipc::read_batch(bytes.value(0))?)?;
                let ticket = seal_attach(
                    &disp,
                    &signing_key,
                    max_ttl_seconds,
                    request,
                    &ctx.auth,
                    unix_now() as i64,
                )?;
                Ok(Some(wire::to_result_batch(ticket)?))
            },
        ))
}

/// Options decoded to strings, for tests and diagnostics that never log.
#[cfg(test)]
fn options_to_strings(ipc_bytes: &[u8]) -> std::collections::BTreeMap<String, String> {
    if ipc_bytes.is_empty() {
        return std::collections::BTreeMap::new();
    }
    let batch = ipc::read_batch(ipc_bytes).unwrap();
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), string_value(batch.column(i)).unwrap()))
        .collect()
}

#[cfg(test)]
mod vector_tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn vectors() -> Value {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/attach_ticket_vectors.json"
        );
        let text = String::from_utf8(std::fs::read(path).unwrap()).unwrap();
        serde_json::from_str(&text).unwrap()
    }

    fn b64(v: &Value) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(v.as_str().unwrap())
            .unwrap()
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn to_hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn key_of(case: &Value, defaults: &Value) -> Vec<u8> {
        b64(case
            .get("signing_key_b64")
            .unwrap_or(&defaults["signing_key_b64"]))
    }

    fn now_of(case: &Value, defaults: &Value) -> f64 {
        case.get("now")
            .unwrap_or(&defaults["now"])
            .as_f64()
            .unwrap()
    }

    fn principal_of(case: &Value) -> Option<&str> {
        case["principal"].as_str()
    }

    #[test]
    fn mint_vectors_reproduce_byte_for_byte() {
        let v = vectors();
        let mint = v["mint"].as_array().unwrap();
        assert!(!mint.is_empty());
        for case in mint {
            let name = case["name"].as_str().unwrap();
            let principal = case["principal"].as_str().unwrap();
            let claims = AttachTicketClaims {
                issued_at: case["issued_at"].as_i64().unwrap(),
                expires_at: case["expires_at"].as_i64().unwrap(),
                ticket_id: case["ticket_id"].as_str().unwrap().into(),
                catalog_name: case["catalog_name"].as_str().unwrap().into(),
                data_version_spec: case["data_version_spec"].as_str().unwrap().into(),
                implementation_version: case["implementation_version"].as_str().unwrap().into(),
                options_ipc: b64(&case["options_ipc_b64"]),
            };
            assert_eq!(
                to_hex(&attach_ticket_aad(principal)),
                case["aad_hex"].as_str().unwrap(),
                "{name}: aad"
            );
            assert_eq!(
                to_hex(&encode_payload(&claims).unwrap()),
                case["payload_hex"].as_str().unwrap(),
                "{name}: payload"
            );
            let nonce: [u8; NONCE_LEN] =
                hex(case["nonce_hex"].as_str().unwrap()).try_into().unwrap();
            let token =
                mint_with_nonce(&key_of(case, &v["defaults"]), principal, &claims, &nonce).unwrap();
            assert_eq!(token, case["token"].as_str().unwrap(), "{name}: token");
        }
    }

    #[test]
    fn accept_vectors_open_to_their_claims() {
        let v = vectors();
        for case in v["accept"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let claims = open_attach_ticket(
                &key_of(case, &v["defaults"]),
                case["token"].as_str().unwrap(),
                principal_of(case),
                now_of(case, &v["defaults"]),
            )
            .unwrap_or_else(|e| panic!("{name}: {e}"));
            let want = &case["claims"];
            assert_eq!(
                claims.issued_at,
                want["issued_at"].as_i64().unwrap(),
                "{name}"
            );
            assert_eq!(
                claims.expires_at,
                want["expires_at"].as_i64().unwrap(),
                "{name}"
            );
            assert_eq!(
                claims.ticket_id,
                want["ticket_id"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(
                claims.catalog_name,
                want["catalog_name"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(
                claims.data_version_spec,
                want["data_version_spec"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(
                claims.implementation_version,
                want["implementation_version"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(claims.options_ipc, b64(&want["options_ipc_b64"]), "{name}");
        }
    }

    #[test]
    fn reject_vectors_are_refused_with_their_kind() {
        let v = vectors();
        let reject = v["reject"].as_array().unwrap();
        assert!(reject.len() >= 20);
        for case in reject {
            let name = case["name"].as_str().unwrap();
            let err = open_attach_ticket(
                &key_of(case, &v["defaults"]),
                case["token"].as_str().unwrap(),
                principal_of(case),
                now_of(case, &v["defaults"]),
            )
            .err()
            .unwrap_or_else(|| panic!("{name}: accepted"));
            assert_eq!(
                err.error_kind.as_deref(),
                case["error_kind"].as_str(),
                "{name}: {err}"
            );
            assert!(
                !err.message.contains(ATTACH_TICKET_PREFIX),
                "{name}: the error must not quote the ticket"
            );
        }
    }

    /// A one-row all-Utf8 options record, as DuckDB sends string options.
    fn options_batch(map: &serde_json::Map<String, Value>) -> Vec<u8> {
        let fields: Vec<Field> = map
            .keys()
            .map(|k| Field::new(k, DataType::Utf8, true))
            .collect();
        let cols: Vec<ArrayRef> = map
            .values()
            .map(|v| Arc::new(StringArray::from(vec![v.as_str().unwrap()])) as ArrayRef)
            .collect();
        ipc::write_batch(&RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap())
            .unwrap()
    }

    fn auth_for(principal: Option<&str>) -> AuthContext {
        match principal {
            Some(p) => AuthContext::for_principal("grant", p),
            None => AuthContext::anonymous(),
        }
    }

    #[test]
    fn redeem_vectors_restore_or_refuse() {
        let v = vectors();
        let redeem = v["redeem"].as_array().unwrap();
        assert!(!redeem.is_empty());
        for case in redeem {
            let name = case["name"].as_str().unwrap();
            let request = CatalogAttachRequest {
                name: "whatever_the_runner_typed".into(),
                options: Some(Bytes(options_batch(case["options"].as_object().unwrap()))),
                data_version_spec: None,
                implementation_version: None,
                client_capabilities: Some(Bytes(vec![1, 2, 3])),
            };
            let key = key_of(case, &v["defaults"]);
            let got = redeem_attach_ticket(
                &request,
                Some(&key),
                &auth_for(principal_of(case)),
                now_of(case, &v["defaults"]),
            );
            match (case.get("error_kind"), got) {
                (Some(kind), Err(e)) => {
                    assert_eq!(e.error_kind.as_deref(), kind.as_str(), "{name}: {e}")
                }
                (Some(kind), Ok(r)) => panic!("{name}: expected {kind}, got {r:?}"),
                (None, Err(e)) => panic!("{name}: {e}"),
                (None, Ok(restored)) => {
                    let want = &case["result"];
                    if want.is_null() {
                        assert!(restored.is_none(), "{name}: request must be untouched");
                        continue;
                    }
                    let r = restored.unwrap_or_else(|| panic!("{name}: not redeemed"));
                    assert_eq!(r.name, want["catalog_name"].as_str().unwrap(), "{name}");
                    assert_eq!(
                        r.data_version_spec.as_deref(),
                        want["data_version_spec"].as_str(),
                        "{name}"
                    );
                    assert_eq!(
                        r.implementation_version.as_deref(),
                        want["implementation_version"].as_str(),
                        "{name}"
                    );
                    let options =
                        options_to_strings(r.options.as_ref().map_or(&[][..], |b| &b.0[..]));
                    let want_options: BTreeMap<String, String> = want["options"]
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                        .collect();
                    assert_eq!(options, want_options, "{name}");
                    assert_eq!(
                        r.client_capabilities.as_ref().map(|b| b.0.clone()),
                        Some(vec![1, 2, 3]),
                        "{name}: client_capabilities kept"
                    );
                }
            }
        }
    }

    #[test]
    fn a_worker_without_a_key_refuses_every_ticket() {
        let v = vectors();
        let case = &v["redeem"][0];
        let request = CatalogAttachRequest {
            name: "x".into(),
            options: Some(Bytes(options_batch(case["options"].as_object().unwrap()))),
            data_version_spec: None,
            implementation_version: None,
            client_capabilities: None,
        };
        let err =
            redeem_attach_ticket(&request, None, &auth_for(Some("alice")), 1.79e9).unwrap_err();
        assert_eq!(err.error_kind.as_deref(), Some("attach_ticket_invalid"));
    }

    #[test]
    fn options_without_a_ticket_leave_the_request_alone() {
        let mut map = serde_json::Map::new();
        map.insert("region".into(), Value::String("eu".into()));
        let request = CatalogAttachRequest {
            name: "x".into(),
            options: Some(Bytes(options_batch(&map))),
            data_version_spec: None,
            implementation_version: None,
            client_capabilities: None,
        };
        assert!(
            redeem_attach_ticket(&request, Some(b"k"), &auth_for(None), 0.0)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn non_string_ticket_is_invalid() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                ATTACH_TICKET_OPTION,
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![7])) as ArrayRef],
        )
        .unwrap();
        let request = CatalogAttachRequest {
            name: "x".into(),
            options: Some(Bytes(ipc::write_batch(&batch).unwrap())),
            data_version_spec: None,
            implementation_version: None,
            client_capabilities: None,
        };
        let err =
            redeem_attach_ticket(&request, Some(b"k"), &auth_for(Some("alice")), 0.0).unwrap_err();
        assert_eq!(err.error_kind.as_deref(), Some("attach_ticket_invalid"));
    }

    #[test]
    fn fresh_tickets_round_trip_and_differ() {
        let claims = AttachTicketClaims {
            issued_at: 100,
            expires_at: 0,
            ticket_id: new_ticket_id(),
            catalog_name: "c".into(),
            data_version_spec: String::new(),
            implementation_version: String::new(),
            options_ipc: Vec::new(),
        };
        let a = mint_attach_ticket(b"key", "alice", &claims).unwrap();
        let b = mint_attach_ticket(b"key", "alice", &claims).unwrap();
        assert_ne!(a, b, "a fresh nonce per ticket");
        assert_eq!(
            open_attach_ticket(b"key", &a, Some("alice"), 1e12).unwrap(),
            claims
        );
        assert!(is_ticket_id(&claims.ticket_id));
    }
}
