// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Where a worker lives, parsed from a `LOCATION` string.
//!
//! One string names both the transport and its address, exactly as the DuckDB
//! extension's `ATTACH … (LOCATION '…')` option does. Sharing the spelling is
//! the point: the same `${VGI_TEST_WORKER}` value drives the extension and this
//! client, so a test corpus written for one runs against the other.
//!
//! # Schemes
//!
//! | Prefix | Transport |
//! |---|---|
//! | `http://`, `https://` | HTTP |
//! | `unix://` | AF_UNIX, worker started out of band |
//! | `launch:` | AF_UNIX, worker spawned and shared by the launcher |
//! | `tcp://` | TCP |
//! | anything else | subprocess (the string is a command line) |
//!
//! The extension also accepts `oci://`, `github://`, `github-auto://` and
//! `worker:`. Those are deliberately **not** handled here — each is a resolver
//! that produces a subprocess command rather than a transport of its own, and
//! [`VgiLocation::parse`] rejects them by name so the failure says which
//! feature is missing rather than trying to exec `oci://…`.

use std::path::PathBuf;

use vgi_rpc::errors::{Result, RpcError};

/// A parsed `LOCATION`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VgiLocation {
    /// Spawn a worker as a child process; the payload is its argv.
    Subprocess(Vec<String>),
    /// A worker serving VGI over HTTP.
    Http(String),
    /// A worker listening on an AF_UNIX socket, started out of band.
    Unix(PathBuf),
    /// A worker listening on TCP.
    Tcp {
        /// Host to dial.
        host: String,
        /// Port to dial.
        port: u16,
    },
    /// A worker spawned (and shared system-wide) by the launcher; argv payload.
    Launch(Vec<String>),
}

impl VgiLocation {
    /// Parse a `LOCATION` string.
    pub fn parse(location: &str) -> Result<Self> {
        let s = location.trim();
        if s.is_empty() {
            return Err(RpcError::value_error("LOCATION is empty"));
        }

        if let Some(rest) = s.strip_prefix("launch:") {
            return Ok(Self::Launch(parse_launch_argv(rest)?));
        }
        if s.starts_with("http://") || s.starts_with("https://") {
            return Ok(Self::Http(s.to_string()));
        }
        if let Some(path) = s.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(RpcError::value_error("unix:// LOCATION has no path"));
            }
            return Ok(Self::Unix(PathBuf::from(path)));
        }
        if let Some(hostport) = s.strip_prefix("tcp://") {
            let (host, port) = hostport
                .rsplit_once(':')
                .ok_or_else(|| RpcError::value_error("tcp:// LOCATION needs host:port"))?;
            let port: u16 = port.parse().map_err(|_| {
                RpcError::value_error(format!("tcp:// LOCATION has a bad port: {port:?}"))
            })?;
            if host.is_empty() {
                return Err(RpcError::value_error("tcp:// LOCATION has no host"));
            }
            return Ok(Self::Tcp {
                host: host.to_string(),
                port,
            });
        }

        // Name the unsupported resolvers rather than trying to exec them.
        for scheme in [
            "oci://",
            "docker://",
            "github://",
            "github-auto://",
            "worker:",
        ] {
            if s.starts_with(scheme) {
                return Err(RpcError::value_error(format!(
                    "{scheme} LOCATIONs are not supported by this client \
                     (the DuckDB extension resolves them to a local worker command; \
                     resolve it yourself and pass the command)"
                )));
            }
        }

        Ok(Self::Subprocess(parse_launch_argv(s)?))
    }

    /// A short human-readable label, for errors and plan display.
    pub fn label(&self) -> String {
        match self {
            Self::Subprocess(argv) => argv.first().cloned().unwrap_or_else(|| "worker".into()),
            Self::Http(url) => url.clone(),
            Self::Unix(p) => format!("unix://{}", p.display()),
            Self::Tcp { host, port } => format!("tcp://{host}:{port}"),
            Self::Launch(argv) => {
                format!("launch:{}", argv.first().map(String::as_str).unwrap_or(""))
            }
        }
    }
}

/// Split a command line the way the extension's `ParseLaunchArgv` does.
///
/// POSIX shell quoting, and it must agree byte-for-byte with
/// `vgi/src/vgi_launcher_internal.cpp` — the argv it produces is hashed into
/// the launcher's rendezvous key, so a client that tokenises differently gets a
/// different hash and silently starts a *second* worker instead of sharing the
/// warm one.
///
/// Supported: whitespace separation; `"…"` with `\"`, `\\`, `\$`, `` \` `` and
/// `\<newline>` escapes (any other backslash inside double quotes stays
/// literal, per POSIX); `'…'` raw with no escapes; bare backslash escapes
/// outside quotes.
///
/// Windows deliberately diverges the same way the C++ does: a backslash is a
/// path separator there, not an escape, so `C:\path\to\worker` survives.
fn parse_launch_argv(payload: &str) -> Result<Vec<String>> {
    #[derive(PartialEq)]
    enum State {
        Default,
        InDouble,
        InSingle,
    }

    let mut out: Vec<String> = Vec::new();
    let mut token = String::new();
    let mut has_token = false;
    let mut state = State::Default;

    let chars: Vec<char> = payload.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match state {
            State::Default => match c {
                ' ' | '\t' | '\n' => {
                    if has_token {
                        out.push(std::mem::take(&mut token));
                        has_token = false;
                    }
                }
                '"' => {
                    state = State::InDouble;
                    has_token = true; // an empty "" is still a token
                }
                '\'' => {
                    state = State::InSingle;
                    has_token = true;
                }
                '\\' if !cfg!(windows) => {
                    if i + 1 >= chars.len() {
                        return Err(RpcError::value_error("trailing backslash in LOCATION argv"));
                    }
                    i += 1;
                    token.push(chars[i]);
                    has_token = true;
                }
                _ => {
                    token.push(c);
                    has_token = true;
                }
            },
            State::InDouble => {
                if c == '"' {
                    state = State::Default;
                } else if c == '\\' && i + 1 < chars.len() && !cfg!(windows) {
                    let next = chars[i + 1];
                    if matches!(next, '"' | '\\' | '$' | '`' | '\n') {
                        token.push(next);
                        i += 1;
                    } else {
                        token.push('\\');
                    }
                } else {
                    token.push(c);
                }
            }
            State::InSingle => {
                if c == '\'' {
                    state = State::Default;
                } else {
                    token.push(c);
                }
            }
        }
        i += 1;
    }

    if state != State::Default {
        return Err(RpcError::value_error("unterminated quote in LOCATION argv"));
    }
    if has_token {
        out.push(token);
    }
    if out.is_empty() {
        return Err(RpcError::value_error("LOCATION has an empty argv"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(s: &str) -> Vec<String> {
        parse_launch_argv(s).expect("parses")
    }

    #[test]
    fn schemes_route_to_the_right_transport() {
        assert!(matches!(
            VgiLocation::parse("http://h:1").unwrap(),
            VgiLocation::Http(_)
        ));
        assert!(matches!(
            VgiLocation::parse("https://h/v").unwrap(),
            VgiLocation::Http(_)
        ));
        assert_eq!(
            VgiLocation::parse("unix:///tmp/w.sock").unwrap(),
            VgiLocation::Unix(PathBuf::from("/tmp/w.sock"))
        );
        assert_eq!(
            VgiLocation::parse("tcp://127.0.0.1:9000").unwrap(),
            VgiLocation::Tcp {
                host: "127.0.0.1".into(),
                port: 9000
            }
        );
        assert_eq!(
            VgiLocation::parse("launch:python -m w").unwrap(),
            VgiLocation::Launch(vec!["python".into(), "-m".into(), "w".into()])
        );
        assert_eq!(
            VgiLocation::parse("/usr/bin/worker --flag").unwrap(),
            VgiLocation::Subprocess(vec!["/usr/bin/worker".into(), "--flag".into()])
        );
    }

    #[test]
    fn unsupported_resolvers_name_themselves() {
        for s in [
            "oci://img:tag",
            "docker://img",
            "github://o/r@v/a",
            "github-auto://o/r@v",
            "worker:https://x/y.js",
        ] {
            let err = VgiLocation::parse(s).unwrap_err().to_string();
            assert!(err.contains("not supported"), "{s} -> {err}");
        }
    }

    // The tokenizer cases below mirror `ParseLaunchArgv` in
    // vgi/src/vgi_launcher_internal.cpp. They are hash-parity critical.

    #[test]
    fn words_split_on_any_whitespace() {
        assert_eq!(argv("a b\tc\nd"), vec!["a", "b", "c", "d"]);
        assert_eq!(argv("   a   b   "), vec!["a", "b"]);
    }

    #[test]
    fn single_quotes_are_raw() {
        assert_eq!(argv(r#"'a b' 'c\d'"#), vec!["a b", r"c\d"]);
    }

    #[test]
    fn double_quotes_honour_only_the_posix_escapes() {
        assert_eq!(argv(r#""a\"b""#), vec![r#"a"b"#]);
        assert_eq!(argv(r#""a\\b""#), vec![r"a\b"]);
        assert_eq!(argv(r#""a\$b""#), vec!["a$b"]);
        // Any other backslash inside double quotes stays literal.
        assert_eq!(argv(r#""a\nb""#), vec![r"a\nb"]);
    }

    #[cfg_attr(windows, ignore = "backslash is a path separator on Windows")]
    #[test]
    fn bare_backslash_escapes_outside_quotes() {
        assert_eq!(argv(r"a\ b"), vec!["a b"]);
        assert_eq!(argv(r"a\\b"), vec![r"a\b"]);
    }

    #[test]
    fn an_empty_quoted_string_is_still_an_argument() {
        assert_eq!(argv(r#"a "" b"#), vec!["a", "", "b"]);
    }

    #[test]
    fn malformed_input_is_rejected() {
        assert!(parse_launch_argv(r#"a "b"#).is_err());
        assert!(parse_launch_argv("a 'b").is_err());
        assert!(parse_launch_argv("").is_err());
        assert!(VgiLocation::parse("").is_err());
        assert!(VgiLocation::parse("unix://").is_err());
        assert!(VgiLocation::parse("tcp://nohost").is_err());
        assert!(VgiLocation::parse("tcp://h:99999").is_err());
    }

    #[cfg_attr(windows, ignore = "backslash is a path separator on Windows")]
    #[test]
    fn trailing_backslash_is_an_error() {
        assert!(parse_launch_argv(r"a\").is_err());
    }
}
