// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The ATTACH options of an attach — secret ones included — as functions and
//! transaction hooks read them.
//!
//! ```sql
//! ATTACH 'cat' AS c (TYPE vgi, LOCATION '...', region 'eu', api_key 'sk-...');
//! ```
//!
//! [`attach_options`] decodes the options of the attach an `attach_opaque_data`
//! value belongs to: pass it
//! [`BindParams::attach_opaque_data`](crate::BindParams::attach_opaque_data),
//! [`ProcessParams::attach_opaque_data`](crate::ProcessParams::attach_opaque_data)
//! (every function kind), or the `attach_opaque_data` argument of a
//! [`CatalogTransactions`](crate::catalog::CatalogTransactions) hook.
//!
//! ```no_run
//! # fn f(params: &vgi::ProcessParams) -> vgi::Result<()> {
//! let opts = vgi::attach_options::attach_options(
//!     params.attach_opaque_data.as_deref().unwrap_or_default(),
//! )?;
//! let api_key: Option<String> = opts.as_ref().and_then(|o| o.string("api_key"));
//! # Ok(()) }
//! ```
//!
//! ## Secret options never travel in plaintext
//!
//! An option declared [`secret`](crate::catalog::AttachOptionFlags::secret)
//! (an API key, a token, a password) is **encrypted by the SDK** inside
//! `attach_opaque_data` with a key the client never has, and decrypted only
//! by [`attach_options`]. Catalog authors never handle the encryption, and
//! must never copy a secret into their own attach bytes
//! ([`CatalogModel::attach_payload`](crate::catalog::CatalogModel::attach_payload)
//! refuses a payload that contains a secret value) or keep it server-side just
//! to reach their functions — this accessor is the standard way.
//!
//! - **Key**: `K = HKDF-SHA256(root, salt = "", info =
//!   "vgi.attach_secret_options.v1", 32)`. `root` is the key configured with
//!   [`Worker::attach_secret_key`](crate::Worker::attach_secret_key), else the
//!   HTTP signing key (`VGI_SIGNING_KEY`, or the one the worker generated) on
//!   HTTP, else the **host-local key** on stdio / unix-socket / launcher
//!   workers: 32 random bytes in `$VGI_ATTACH_SECRET_KEY_FILE`, else
//!   `$XDG_RUNTIME_DIR/vgi/attach-secret-options.key`, else
//!   `<temp dir>/vgi-<user>/attach-secret-options.key` (directory 0700, file
//!   0600, owned by the user; created atomically, never overwritten; shared
//!   by every VGI SDK's workers on the host; read only when a secret option
//!   has a value).
//! - **AEAD**: XChaCha20-Poly1305, a random nonce per attach, envelope
//!   `version || nonce || ciphertext || tag`, AAD
//!   `"vgi.attach_secret_options.v1" 0x00 || attach_uuid` — the attach's own
//!   fresh 16-byte id, so a blob moved into another attach value does not open.
//! - **Failure**: a blob that does not open is the uniform
//!   `attach_opaque_data not recognized` rejection. An ATTACH that supplies a
//!   secret value when no key can be obtained fails, naming the key file.
//! - On HTTP the whole value is additionally sealed for the caller
//!   ([`crate::opaque`]); the secret part is encrypted on every transport.
//!
//! Secret values, the key and the blob are never logged.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use vgi_rpc::{Result, RpcError};

use crate::ipc;

/// HKDF `info` and the AAD prefix of the secret-options envelope.
const LABEL: &[u8] = b"vgi.attach_secret_options.v1";
/// Envelope version byte (XChaCha20-Poly1305).
const ENVELOPE_VERSION: u8 = 1;
/// Marks the SDK's options trailer at the end of an attach value.
const MAGIC: &[u8; 8] = b"VGIOPT01";
/// Length of the attach uuid the envelope is bound to.
pub(crate) const ATTACH_UUID_LEN: usize = 16;
const KEY_ID_LEN: usize = 8;
const KEY_LEN: usize = 32;

/// The environment variable naming the host-local key file.
pub const KEY_FILE_ENV: &str = "VGI_ATTACH_SECRET_KEY_FILE";

/// The options of one attach (see the [module docs](self)): one row, one
/// column per option the ATTACH supplied (or, for a catalog with
/// [`attach_options_default_batch`](crate::catalog::CatalogModel::attach_options_default_batch),
/// per declared option with its default), secret options decrypted.
#[derive(Clone, Debug)]
pub struct AttachOptions {
    batch: RecordBatch,
}

impl AttachOptions {
    /// The one-row options record.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// The 1-row array of option `name` (case-insensitive), or `None` when it
    /// is absent or NULL.
    pub fn get(&self, name: &str) -> Option<&ArrayRef> {
        let schema = self.batch.schema();
        let index = schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(name))?;
        let col = self.batch.column(index);
        (!col.is_empty() && !col.is_null(0)).then_some(col)
    }

    fn cast(&self, name: &str, to: &DataType) -> Option<ArrayRef> {
        arrow_cast::cast(self.get(name)?, to).ok()
    }

    /// Option `name` as text (any type, cast).
    pub fn string(&self, name: &str) -> Option<String> {
        let col = self.cast(name, &DataType::Utf8)?;
        Some(
            col.as_any()
                .downcast_ref::<arrow_array::StringArray>()?
                .value(0)
                .to_string(),
        )
    }

    /// Option `name` as a boolean.
    pub fn boolean(&self, name: &str) -> Option<bool> {
        let col = self.cast(name, &DataType::Boolean)?;
        Some(
            col.as_any()
                .downcast_ref::<arrow_array::BooleanArray>()?
                .value(0),
        )
    }

    /// Option `name` as an `i64`.
    pub fn int64(&self, name: &str) -> Option<i64> {
        let col = self.cast(name, &DataType::Int64)?;
        Some(
            col.as_any()
                .downcast_ref::<arrow_array::Int64Array>()?
                .value(0),
        )
    }
}

/// The options of the attach `attach_opaque_data` belongs to, secret values
/// decrypted; `None` when the attach carries none. A secret blob that does
/// not open is the uniform `attach_opaque_data not recognized` error.
pub fn attach_options(attach_opaque_data: &[u8]) -> Result<Option<AttachOptions>> {
    let (core, trailer) = split_trailer(attach_opaque_data);
    let base = match trailer.as_ref().filter(|t| !t.plain.is_empty()) {
        Some(t) => Some(ipc::read_batch(t.plain).map_err(|_| rejected())?),
        None => format_a_batch(core),
    };
    let secret = match trailer.as_ref().filter(|t| !t.sealed.is_empty()) {
        Some(t) => Some(open_secrets(t)?),
        None => None,
    };
    let merged = match (base, secret) {
        (None, None) => return Ok(None),
        (Some(b), None) => b,
        (None, Some(s)) => s,
        (Some(b), Some(s)) => merge(&b, &s)?,
    };
    Ok(Some(AttachOptions { batch: merged }))
}

/// The value without the SDK's options trailer: what the attach encodings
/// (the declarative-options record, a payload, a version) are parsed from.
pub(crate) fn strip_trailer(attach_opaque_data: &[u8]) -> &[u8] {
    split_trailer(attach_opaque_data).0
}

/// The `<16-byte id>\0<ipc>` declarative-options record, if `core` is one.
fn format_a_batch(core: &[u8]) -> Option<RecordBatch> {
    if core.len() <= 17 || core[16] != 0 || core.starts_with(b"\x00sec\x00") {
        return None;
    }
    ipc::read_batch(&core[17..]).ok()
}

fn rejected() -> RpcError {
    crate::opaque::opaque_data_rejected("attach_opaque_data")
}

struct Trailer<'a> {
    uuid: &'a [u8],
    plain: &'a [u8],
    sealed: &'a [u8],
    key_id: &'a [u8],
}

fn split_trailer(value: &[u8]) -> (&[u8], Option<Trailer<'_>>) {
    let parse = || -> Option<(&[u8], Trailer<'_>)> {
        let rest = value.strip_suffix(MAGIC.as_slice())?;
        let (rest, len) = rest.split_at(rest.len().checked_sub(4)?);
        let len = u32::from_be_bytes(len.try_into().ok()?) as usize;
        let (core, body) = rest.split_at(rest.len().checked_sub(len)?);
        let (uuid, body) = body.split_at_checked(ATTACH_UUID_LEN)?;
        let (plain_len, body) = body.split_at_checked(4)?;
        let (plain, body) =
            body.split_at_checked(u32::from_be_bytes(plain_len.try_into().ok()?) as usize)?;
        let (sealed_len, body) = body.split_at_checked(4)?;
        let (sealed, body) =
            body.split_at_checked(u32::from_be_bytes(sealed_len.try_into().ok()?) as usize)?;
        (body.len() == KEY_ID_LEN).then_some((
            core,
            Trailer {
                uuid,
                plain,
                sealed,
                key_id: body,
            },
        ))
    };
    match parse() {
        Some((core, t)) => (core, Some(t)),
        None => (value, None),
    }
}

/// Build the trailer the dispatcher appends to an attach value.
pub(crate) fn encode_trailer(
    uuid: &[u8; ATTACH_UUID_LEN],
    plain: Option<&RecordBatch>,
    secret: Option<&RecordBatch>,
    root: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let plain = match plain {
        Some(b) => ipc::write_batch(b)?,
        None => Vec::new(),
    };
    let (sealed, key_id) = match secret {
        Some(b) => {
            let key = derive_for(root)?;
            let sealed = vgi_rpc::crypto::seal_bytes(
                &ipc::write_batch(b)?,
                &key,
                &aad(uuid),
                ENVELOPE_VERSION,
            );
            (sealed, key_id(&key))
        }
        None => (Vec::new(), [0u8; KEY_ID_LEN]),
    };
    let mut body = uuid.to_vec();
    body.extend((plain.len() as u32).to_be_bytes());
    body.extend(&plain);
    body.extend((sealed.len() as u32).to_be_bytes());
    body.extend(&sealed);
    body.extend(key_id);
    let mut out = body.clone();
    out.extend((body.len() as u32).to_be_bytes());
    out.extend(MAGIC);
    Ok(out)
}

fn aad(uuid: &[u8]) -> Vec<u8> {
    let mut aad = LABEL.to_vec();
    aad.push(0);
    aad.extend_from_slice(uuid);
    aad
}

fn open_secrets(t: &Trailer<'_>) -> Result<RecordBatch> {
    let key = key_for_id(t.key_id).ok_or_else(rejected)?;
    let plain = vgi_rpc::crypto::open_bytes(t.sealed, &key, &aad(t.uuid), ENVELOPE_VERSION)
        .map_err(|_| rejected())?;
    ipc::read_batch(&plain).map_err(|_| rejected())
}

fn merge(base: &RecordBatch, secret: &RecordBatch) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for (f, c) in base.schema().fields().iter().zip(base.columns()) {
        if secret
            .schema()
            .fields()
            .iter()
            .any(|s| s.name().eq_ignore_ascii_case(f.name()))
        {
            continue;
        }
        fields.push(f.as_ref().clone());
        cols.push(c.clone());
    }
    for (f, c) in secret.schema().fields().iter().zip(secret.columns()) {
        fields.push(f.as_ref().clone());
        cols.push(c.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| RpcError::runtime_error(format!("attach options: {e}")))
}

// ------------------------------------------------------------------ keys

/// HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &[&[u8]]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    for d in data {
        inner.update(d);
    }
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// HKDF-SHA256 with an empty salt and `L = 32` (one block).
fn hkdf_sha256(ikm: &[u8], info: &[u8]) -> [u8; KEY_LEN] {
    let prk = hmac_sha256(&[0u8; 32], &[ikm]);
    hmac_sha256(&prk, &[info, &[1u8]])
}

/// The AEAD key for a root key: never the root itself.
pub(crate) fn derive_key(root: &[u8]) -> [u8; KEY_LEN] {
    hkdf_sha256(root, LABEL)
}

fn key_id(key: &[u8; KEY_LEN]) -> [u8; KEY_ID_LEN] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"vgi.attach_secret_options.key_id.v1\x00");
    h.update(key);
    let d = h.finalize();
    let mut out = [0u8; KEY_ID_LEN];
    out.copy_from_slice(&d[..KEY_ID_LEN]);
    out
}

/// Derived keys this process can open with, by key id. A process may host
/// several workers (each with its own root), so lookup is by id.
fn registry() -> &'static Mutex<HashMap<[u8; KEY_ID_LEN], [u8; KEY_LEN]>> {
    static KEYS: OnceLock<Mutex<HashMap<[u8; KEY_ID_LEN], [u8; KEY_LEN]>>> = OnceLock::new();
    KEYS.get_or_init(Default::default)
}

/// Make `root`'s derived key available to [`attach_options`] in this process.
pub(crate) fn register_root(root: &[u8]) {
    let key = derive_key(root);
    registry().lock().unwrap().insert(key_id(&key), key);
}

/// The key to encrypt with: `root`'s, or the host-local key's.
fn derive_for(root: Option<&[u8]>) -> Result<[u8; KEY_LEN]> {
    let key = match root {
        Some(r) => derive_key(r),
        None => derive_key(&host_local_root()?),
    };
    registry().lock().unwrap().insert(key_id(&key), key);
    Ok(key)
}

fn key_for_id(id: &[u8]) -> Option<[u8; KEY_LEN]> {
    let id: [u8; KEY_ID_LEN] = id.try_into().ok()?;
    if let Some(k) = registry().lock().unwrap().get(&id) {
        return Some(*k);
    }
    // Another process of this deployment encrypted with the host-local key.
    let key = derive_key(&host_local_root().ok()?);
    registry().lock().unwrap().insert(key_id(&key), key);
    (key_id(&key) == id).then_some(key)
}

/// The host-local root key, read (or created) once per process.
fn host_local_root() -> Result<Vec<u8>> {
    static ROOT: OnceLock<Vec<u8>> = OnceLock::new();
    if let Some(r) = ROOT.get() {
        return Ok(r.clone());
    }
    let key = load_or_create_key_file(&host_local_key_path())?;
    Ok(ROOT.get_or_init(|| key).clone())
}

/// Where the host-local key lives (identical across VGI SDKs):
/// `$VGI_ATTACH_SECRET_KEY_FILE`, else
/// `$XDG_RUNTIME_DIR/vgi/attach-secret-options.key`, else
/// `<temp dir>/vgi-<user>/attach-secret-options.key`.
pub fn host_local_key_path() -> PathBuf {
    if let Some(p) = std::env::var_os(KEY_FILE_ENV).filter(|p| !p.is_empty()) {
        return PathBuf::from(p);
    }
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|d| !d.is_empty())
        .map(PathBuf::from)
        .filter(|d| d.is_dir())
    {
        return dir.join("vgi").join("attach-secret-options.key");
    }
    let user = ["USER", "LOGNAME", "USERNAME"]
        .iter()
        .find_map(|v| std::env::var(v).ok().filter(|u| !u.is_empty()))
        .unwrap_or_else(|| "unknown".to_string());
    std::env::temp_dir()
        .join(format!("vgi-{user}"))
        .join("attach-secret-options.key")
}

fn key_error(path: &Path, why: &str) -> RpcError {
    RpcError::runtime_error(format!(
        "cannot use the host-local attach secret key {}: {why}. Set VGI_SIGNING_KEY, or point \
         {KEY_FILE_ENV} at a 32-byte key file only this user can read",
        path.display()
    ))
}

/// Read the 32-byte key at `path`, creating it (and its 0700 directory)
/// atomically when absent. Refuses a file or directory another user owns or
/// that group/other can access, and a file that is not exactly 32 bytes.
pub(crate) fn load_or_create_key_file(path: &Path) -> Result<Vec<u8>> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !dir.exists() {
        create_private_dir(dir)
            .map_err(|e| key_error(path, &format!("creating its directory: {e}")))?;
    }
    check_private(dir, true).map_err(|why| key_error(path, &why))?;
    if !path.exists() {
        create_key_file(dir, path).map_err(|e| key_error(path, &format!("creating it: {e}")))?;
    }
    check_private(path, false).map_err(|why| key_error(path, &why))?;
    let bytes = std::fs::read(path).map_err(|e| key_error(path, &format!("reading it: {e}")))?;
    if bytes.len() != KEY_LEN {
        return Err(key_error(path, "it must hold exactly 32 bytes"));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    match std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
    {
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        r => r,
    }
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

/// Write 32 random bytes to a 0600 temp file in `dir`, then hard-link it into
/// place; a key some other process linked first wins and is used.
fn create_key_file(dir: &Path, path: &Path) -> std::io::Result<()> {
    use rand::RngCore;
    use std::io::Write;
    let mut key = [0u8; KEY_LEN];
    rand::rngs::OsRng.fill_bytes(&mut key);
    let mut suffix = [0u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut suffix);
    let tmp = dir.join(format!(
        ".attach-secret-options.{}.tmp",
        suffix
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    ));
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let result = (|| {
        let mut f = opts.open(&tmp)?;
        f.write_all(&key)?;
        f.sync_all()?;
        match std::fs::hard_link(&tmp, path) {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            r => r,
        }
    })();
    let _ = std::fs::remove_file(&tmp);
    result
}

#[cfg(unix)]
fn check_private(path: &Path, is_dir: bool) -> std::result::Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if is_dir != meta.is_dir() || (!is_dir && !meta.is_file()) {
        return Err(if is_dir {
            "its directory is not a directory".into()
        } else {
            "it is not a regular file".into()
        });
    }
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if meta.uid() != uid {
        return Err(format!(
            "{} is owned by another user",
            if is_dir { "its directory" } else { "it" }
        ));
    }
    if meta.mode() & 0o077 != 0 {
        return Err(format!(
            "{} is accessible by group/other (mode {:o})",
            if is_dir { "its directory" } else { "it" },
            meta.mode() & 0o777
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private(path: &Path, is_dir: bool) -> std::result::Result<(), String> {
    let meta = std::fs::metadata(path).map_err(|e| e.to_string())?;
    if is_dir != meta.is_dir() {
        return Err("unexpected file type".into());
    }
    Ok(())
}

/// Split a supplied options record into its non-secret and secret parts
/// (columns whose value is NULL are dropped from the secret part).
pub(crate) fn split_secret(
    options: &RecordBatch,
    secret_names: &[String],
) -> (Option<RecordBatch>, Option<RecordBatch>) {
    let schema = options.schema();
    let is_secret = |n: &str| secret_names.iter().any(|s| s.eq_ignore_ascii_case(n));
    let pick = |want_secret: bool| -> Option<RecordBatch> {
        let keep: Vec<usize> = (0..schema.fields().len())
            .filter(|&i| {
                let secret = is_secret(schema.field(i).name());
                secret == want_secret
                    && (!want_secret || (options.num_rows() > 0 && !options.column(i).is_null(0)))
            })
            .collect();
        (!keep.is_empty())
            .then(|| options.project(&keep).ok())
            .flatten()
    };
    (pick(false), pick(true))
}

/// The byte strings of a secret record's values (for the payload guard).
pub(crate) fn secret_value_bytes(secret: &RecordBatch) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for col in secret.columns() {
        if col.is_empty() || col.is_null(0) {
            continue;
        }
        if let Some(b) = col.as_any().downcast_ref::<arrow_array::BinaryArray>() {
            out.push(b.value(0).to_vec());
        } else if let Ok(s) = arrow_cast::cast(col, &DataType::Utf8) {
            if let Some(s) = s.as_any().downcast_ref::<arrow_array::StringArray>() {
                out.push(s.value(0).as_bytes().to_vec());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    fn record(cols: &[(&str, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(
                cols.iter()
                    .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            )),
            cols.iter()
                .map(|(_, v)| Arc::new(StringArray::from(vec![*v])) as ArrayRef)
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn hkdf_matches_rfc5869_test_case_3() {
        // RFC 5869 A.3: IKM = 0x0b * 22, salt and info empty.
        let okm = hkdf_sha256(&[0x0b; 22], b"");
        let hex: String = okm.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d"
        );
        // The AEAD key is the HKDF subkey, never the root.
        let root = [7u8; 32];
        assert_ne!(derive_key(&root), root);
        assert_eq!(derive_key(&root), hkdf_sha256(&root, LABEL));
    }

    #[test]
    fn secrets_round_trip_and_never_appear_in_the_bytes() {
        let root = b"explicit-root-for-test".to_vec();
        let uuid = [3u8; ATTACH_UUID_LEN];
        let plain = record(&[("region", "eu")]);
        let secret = record(&[("api_key", "sk-very-secret-value")]);
        let mut value = b"cat".to_vec();
        value.extend(encode_trailer(&uuid, Some(&plain), Some(&secret), Some(&root)).unwrap());
        assert!(!value.windows(20).any(|w| w == b"sk-very-secret-value"));
        assert_eq!(strip_trailer(&value), b"cat");
        let opts = attach_options(&value).unwrap().unwrap();
        assert_eq!(opts.string("region").as_deref(), Some("eu"));
        assert_eq!(
            opts.string("API_KEY").as_deref(),
            Some("sk-very-secret-value")
        );
    }

    #[test]
    fn a_blob_moved_into_another_attach_does_not_open() {
        let root = b"root-for-move-test".to_vec();
        let secret = record(&[("api_key", "sk-move")]);
        let a = encode_trailer(&[1; 16], None, Some(&secret), Some(&root)).unwrap();
        let b = encode_trailer(&[2; 16], None, Some(&secret), Some(&root)).unwrap();
        // Splice a's sealed blob into b (keeping b's attach uuid).
        let (_, ta) = split_trailer(&a);
        let (_, tb) = split_trailer(&b);
        let (ta, tb) = (ta.unwrap(), tb.unwrap());
        let mut body = tb.uuid.to_vec();
        body.extend(0u32.to_be_bytes());
        body.extend((ta.sealed.len() as u32).to_be_bytes());
        body.extend(ta.sealed);
        body.extend(tb.key_id);
        let mut moved = body.clone();
        moved.extend((body.len() as u32).to_be_bytes());
        moved.extend(MAGIC);
        let err = attach_options(&moved).unwrap_err();
        assert_eq!(err.message, "attach_opaque_data not recognized");
        // A tampered blob is the same uniform error.
        let mut tampered = a.clone();
        tampered[30] ^= 1;
        assert_eq!(
            attach_options(&tampered).unwrap_err().message,
            "attach_opaque_data not recognized"
        );
        // An unknown key id too.
        let mut unknown = a.clone();
        let n = unknown.len();
        unknown[n - 13] ^= 0xff;
        assert!(attach_options(&unknown).is_err());
    }

    #[test]
    fn values_without_a_trailer_are_unchanged() {
        assert!(attach_options(b"plain-catalog-name").unwrap().is_none());
        assert_eq!(
            strip_trailer(b"\x00sec\x00x\x00abc"),
            b"\x00sec\x00x\x00abc"
        );
    }

    #[cfg(unix)]
    #[test]
    fn host_local_key_file_is_private_reused_and_agreed_on() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!("vgi-keytest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let path = base.join("sub").join("attach-secret-options.key");
        // Concurrent first starts agree on one key.
        let keys: Vec<Vec<u8>> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..8)
                .map(|_| s.spawn(|| load_or_create_key_file(&path).unwrap()))
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(keys.iter().all(|k| k == &keys[0] && k.len() == 32));
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let dmeta = std::fs::metadata(path.parent().unwrap()).unwrap();
        assert_eq!(dmeta.permissions().mode() & 0o777, 0o700);
        // Reused, never overwritten.
        assert_eq!(load_or_create_key_file(&path).unwrap(), keys[0]);
        // No temp files left behind.
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
        // Bad permissions are refused.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = load_or_create_key_file(&path).unwrap_err();
        assert!(err.message.contains("group/other"), "{}", err.message);
        assert!(err.message.contains("VGI_SIGNING_KEY"));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(
            path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert!(load_or_create_key_file(&path).is_err());
        std::fs::set_permissions(
            path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        // A wrong-length key is refused.
        std::fs::write(&path, b"short").unwrap();
        let err = load_or_create_key_file(&path).unwrap_err();
        assert!(err.message.contains("32 bytes"), "{}", err.message);
        let _ = std::fs::remove_dir_all(&base);
    }
}
