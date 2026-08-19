// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Backing off when the server refuses to serve *right now*.
//!
//! `max_workers` is a NORMATIVE cap on how many redemptions a worker will serve
//! at once, and the way a server enforces it over HTTP is `429` +
//! `Retry-After`. The engines that trip it are the ones whose partition count
//! IS their concurrency — DataFusion opens every partition it planned — so a
//! client that cannot read a `429` is not merely missing an optimisation: it
//! turns the protocol's own back-pressure signal into a query failure.
//!
//! What that failure looks like without this module is the reason it is worth
//! writing down. The transport inspects a handful of statuses (`415`, `413`,
//! `401`) and hands the body of everything else to the Arrow IPC decoder, so a
//! `429` whose body is a proxy's `"rate limit exceeded"` surfaces as
//! `empty IPC stream (no schema)` — an error that names neither the status, the
//! endpoint, nor the fact that waiting a second would have fixed it.
//!
//! # Why jitter is not a detail
//!
//! N split readers hit the cap at the same instant, because they were all
//! released by the same planner. Retrying them on a fixed schedule re-forms
//! exactly the synchronized herd the cap exists to break up, and each round
//! arrives more tightly packed than the last. [`RetryPolicy::delay`] therefore
//! draws **full jitter** — uniform over the whole `[0, window)` backoff window
//! rather than a narrow band around it — because for a given mean delay full
//! jitter is the variant that spreads a herd the widest; a ±20% band keeps the
//! herd a herd. The server's `Retry-After`, when it states one, is a FLOOR the
//! jitter is added to, never a value we shorten.
//!
//! # Reading a status from where we stand
//!
//! [`classify_status`] and [`RetryPolicy::decide`] are pure and take the status
//! directly, so the policy is testable and shared. Turning a real response into
//! one of those calls is [`classify_error`], and it is best-effort: the
//! transport crate (`vgi-rpc-client`) does not report the HTTP status of a data
//! call to its caller, so what reaches us is whatever the status *did* to the
//! response. The statuses that do arrive intact are the ones the transport
//! formats into its own message (external-location fetch, upload-URL PUT) plus
//! the `Retry-After` hint it carries on a transient auth failure; anything else
//! is indistinguishable from a malformed response and is treated as fatal,
//! because retrying an unclassifiable failure is how a client turns one bad
//! answer into several. Full fidelity needs the transport to surface the status
//! — see the crate README.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use vgi_rpc::errors::{Result, RpcError};

use crate::transport::{ExchangeStream, ProducerStream, VgiTransport};

/// What one HTTP status means for whether the call can be tried again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    /// 2xx — nothing to decide.
    Success,
    /// `429`: the server is enforcing a concurrency or rate cap. The request
    /// never ran, so retrying it is safe for any method.
    Throttled,
    /// `502` / `503` / `504`: the same retryable class, one hop out. A gateway
    /// that could not reach the worker, or gave up waiting for it.
    Transient,
    /// Everything else. Retrying cannot change the answer.
    Fatal,
}

impl StatusClass {
    /// Whether a call that ended this way may be sent again.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Throttled | Self::Transient)
    }
}

/// Classify one HTTP status.
///
/// `502`/`503`/`504` join `429` rather than sitting with the other 5xx: they
/// are all "this hop could not serve you", produced by the load balancer that
/// also produces the `429`, and a `500` from the worker itself is a real answer
/// that will be identical next time.
pub fn classify_status(status: u16) -> StatusClass {
    match status {
        200..=299 => StatusClass::Success,
        429 => StatusClass::Throttled,
        502 | 503 | 504 => StatusClass::Transient,
        _ => StatusClass::Fatal,
    }
}

/// Parse a `Retry-After` header value against `now`.
///
/// RFC 9110 allows both forms and servers use both: a proxy enforcing a rate
/// limit sends `Retry-After: 2`, while one gating on a window boundary sends
/// the HTTP-date the window ends. Reading only the integer form silently loses
/// the second — and losing it means backing off by the local schedule instead
/// of by the schedule the server actually published.
///
/// An unparseable value is `None`, not an error: the caller then falls back to
/// its own backoff, which is strictly better than failing the query over a
/// malformed header.
pub fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let v = value.trim();
    if v.is_empty() {
        return None;
    }
    // delta-seconds. A negative or absurd value is not honoured as written —
    // it is clamped to zero here and bounded by the caller's own caps, so a
    // hostile header cannot park a scan for a decade.
    if v.bytes().all(|b| b.is_ascii_digit()) {
        return v.parse::<u64>().ok().map(Duration::from_secs);
    }
    let at = parse_http_date(v)?;
    // A date already past means "now"; the server's window has closed while the
    // response was in flight.
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

/// Parse the IMF-fixdate form, `Sun, 06 Nov 1994 08:49:37 GMT`.
///
/// RFC 9110 requires senders to use this form and permits recipients to accept
/// the two obsolete ones; a client that mis-reads an obsolete date would back
/// off by the wrong amount, whereas one that rejects it falls back to its own
/// backoff. So only the mandated form is parsed.
fn parse_http_date(s: &str) -> Option<SystemTime> {
    let b = s.as_bytes();
    if b.len() < 29 || !s.ends_with("GMT") {
        return None;
    }
    // "Sun, 06 Nov 1994 08:49:37 GMT"
    //  0123456789...
    let day: u32 = s.get(5..7)?.trim().parse().ok()?;
    let month = match s.get(8..11)? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = s.get(12..16)?.parse().ok()?;
    let hour: u64 = s.get(17..19)?.parse().ok()?;
    let min: u64 = s.get(20..22)?.parse().ok()?;
    let sec: u64 = s.get(23..25)?.parse().ok()?;
    if day == 0 || day > 31 || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day as i64);
    let secs = days * 86_400 + (hour * 3600 + min * 60 + sec.min(59)) as i64;
    if secs < 0 {
        return None;
    }
    Some(UNIX_EPOCH + Duration::from_secs(secs as u64))
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`), so no date crate is needed for one header.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Why a retry loop stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The failure cannot be fixed by trying again.
    NotRetryable,
    /// Out of attempts.
    AttemptsExhausted,
    /// The next legal attempt is past the caller's total-time budget.
    DeadlineExceeded,
}

/// What to do after one failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Sleep this long, then try again.
    Retry(Duration),
    /// Give up, for this reason.
    Stop(StopReason),
}

/// How hard to try, and for how long.
///
/// Both caps are load-bearing and neither subsumes the other: an attempt cap
/// alone lets a server holding a 10-minute `Retry-After` park a query for half
/// an hour, and a time cap alone lets a fast-failing endpoint be hammered
/// hundreds of times inside it. A permanently-throttling server has to end in a
/// legible error, not a hang.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts, first one included. `1` disables retrying.
    pub max_attempts: u32,
    /// Wall-clock budget across all attempts, sleeps included.
    pub max_elapsed: Duration,
    /// The backoff window before the first retry; doubles per attempt.
    pub base_delay: Duration,
    /// Ceiling on the backoff window (not on `Retry-After`, which is a floor
    /// the server chose and is bounded by `max_elapsed` instead).
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            max_elapsed: Duration::from_secs(60),
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// No retrying at all — one attempt, and whatever it says is the answer.
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }

    /// The backoff window for the `attempt`-th failure (1-based), before jitter.
    fn window(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let scaled = self
            .base_delay
            .saturating_mul(1u32.checked_shl(shift).unwrap_or(u32::MAX));
        scaled.min(self.max_delay)
    }

    /// The sleep after the `attempt`-th failure, given the server's floor and a
    /// jitter fraction in `[0, 1)`.
    ///
    /// Split out from [`Self::decide`] and taking the fraction explicitly so a
    /// test can pin both ends of the distribution; production draws it from
    /// [`jitter_fraction`].
    pub fn delay(&self, attempt: u32, retry_after: Option<Duration>, jitter: f64) -> Duration {
        let window = self.window(attempt).as_secs_f64();
        let jittered = Duration::from_secs_f64(window * jitter.clamp(0.0, 1.0));
        retry_after.unwrap_or(Duration::ZERO).saturating_add(jittered)
    }

    /// Decide what to do after an attempt that ended in `class`.
    ///
    /// `attempt` counts attempts already made (so `1` after the first),
    /// `elapsed` is the time spent so far, and `jitter` is a fraction in
    /// `[0, 1)`. Pure — everything that varies is a parameter.
    pub fn decide(
        &self,
        class: StatusClass,
        attempt: u32,
        elapsed: Duration,
        retry_after: Option<Duration>,
        jitter: f64,
    ) -> RetryDecision {
        if !class.is_retryable() {
            return RetryDecision::Stop(StopReason::NotRetryable);
        }
        if attempt >= self.max_attempts {
            return RetryDecision::Stop(StopReason::AttemptsExhausted);
        }
        let delay = self.delay(attempt, retry_after, jitter);
        // Checked against the budget BEFORE sleeping, so a `Retry-After` longer
        // than the budget fails immediately with a reason rather than sleeping
        // through it only to give up on the far side.
        if elapsed.saturating_add(delay) >= self.max_elapsed {
            return RetryDecision::Stop(StopReason::DeadlineExceeded);
        }
        RetryDecision::Retry(delay)
    }

    /// Run `op` under this policy, retrying while [`classify_error`] says the
    /// failure is one the server invited us to retry.
    ///
    /// `endpoint` names what is being called, and appears in the error a
    /// breached cap produces — a scan that gave up must say *where* it gave up,
    /// since a query fans out across many of them.
    ///
    /// Only for operations whose result is owned. A call that returns a stream
    /// borrowing the client cannot be retried in place (the borrow outlives the
    /// loop that would retry it), so those are retried by the caller that owns
    /// the client, driving this same policy.
    pub fn run<T>(&self, endpoint: &str, mut op: impl FnMut() -> Result<T>) -> Result<T> {
        let start = Instant::now();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let err = match op() {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };
            let (class, retry_after) = classify_error(&err);
            match self.decide(class, attempt, start.elapsed(), retry_after, jitter_fraction()) {
                RetryDecision::Retry(d) => std::thread::sleep(d),
                // A fatal failure is returned as the peer stated it: wrapping it
                // in retry vocabulary would bury the real cause under an
                // apology for not retrying.
                RetryDecision::Stop(StopReason::NotRetryable) => return Err(err),
                RetryDecision::Stop(reason) => {
                    return Err(gave_up(endpoint, attempt, start.elapsed(), reason, &err))
                }
            }
        }
    }
}

/// The error a breached cap produces.
///
/// Names the endpoint, the attempt count and the elapsed time, because the
/// thing an operator needs to know is whether to raise the cap or fix the
/// server — and the last peer error is what says which.
fn gave_up(
    endpoint: &str,
    attempts: u32,
    elapsed: Duration,
    reason: StopReason,
    last: &RpcError,
) -> RpcError {
    let why = match reason {
        StopReason::AttemptsExhausted => "attempt cap reached",
        StopReason::DeadlineExceeded => "retry budget exhausted",
        StopReason::NotRetryable => "not retryable",
    };
    RpcError::new(
        "TransportError",
        format!(
            "{endpoint}: giving up after {attempts} attempt(s) over {:.1}s ({why}); \
             last failure: {last}",
            elapsed.as_secs_f64()
        ),
    )
}

/// Best-effort classification of a failed call.
///
/// See the module docs for why this is best-effort: the status of a data call
/// is not reported to us, so this reads the traces a status leaves. Anything it
/// cannot place is [`StatusClass::Fatal`], which is the safe direction — a
/// retried non-idempotent call is worse than an un-retried throttle.
pub fn classify_error(err: &RpcError) -> (StatusClass, Option<Duration>) {
    // A transient auth failure carries its own Retry-After, already parsed.
    // `AuthUnavailableError` means "I could not determine whether your
    // credential is good", which is exactly a retryable condition and is
    // deliberately not a rejection.
    if err.is_auth_unavailable() {
        let after = err.retry_after_seconds.map(|s| Duration::from_secs(s.into()));
        return (StatusClass::Transient, after);
    }
    if let Some(status) = status_in_message(&err.message) {
        let class = classify_status(status);
        if class.is_retryable() {
            return (class, err.retry_after_seconds.map(|s| Duration::from_secs(s.into())));
        }
        return (StatusClass::Fatal, None);
    }
    (StatusClass::Fatal, None)
}

/// Pull an HTTP status out of a transport error message.
///
/// The transport formats `HTTP <status>` into the errors it raises for the
/// external-location fetch and the upload-URL PUT — both on the data path for a
/// large batch — so those statuses do survive to us in text. Matching on text
/// is not something to be proud of, but the alternative is dropping the one
/// place a real `503` is currently legible.
pub fn status_in_message(msg: &str) -> Option<u16> {
    let rest = msg.split("HTTP ").nth(1)?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.len() != 3 {
        return None;
    }
    digits.parse().ok()
}

/// A jitter fraction in `[0, 1)` from a real per-call entropy source: the wall
/// clock's sub-second component mixed with a thread-local counter, through
/// splitmix64. Not cryptographic — jitter does not need to be — but it must
/// differ between two readers that failed in the same millisecond, which is the
/// whole case it exists for.
pub fn jitter_fraction() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static SEQ: Cell<u64> = const { Cell::new(0) };
    }
    let seq = SEQ.with(|c| {
        let v = c.get().wrapping_add(1);
        c.set(v);
        v
    });
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
        .unwrap_or(0);
    let mut z = nanos
        .wrapping_add(seq.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(std::process::id() as u64);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // 53 bits is the mantissa, so the quotient is exact and never reaches 1.0.
    (z >> 11) as f64 / (1u64 << 53) as f64
}

/// Whether a unary method may be sent twice.
///
/// An allowlist, not a denylist: a method absent from it is not retried, so a
/// method added to the protocol later cannot be retried by accident. The ones
/// listed resolve or read — they leave no state behind, so a duplicate costs a
/// round trip. The ones deliberately absent do not: `catalog_attach` /
/// `catalog_detach` and the transaction verbs move session state, and
/// `aggregate_update` / `table_buffering_process` FOLD INPUT INTO AN
/// ACCUMULATOR, where a duplicate is not a wasted call but a wrong answer.
pub fn method_is_retryable(method: &str) -> bool {
    matches!(
        method,
        "bind" | "table_function_plan" | "catalog_catalogs" | "catalog_version"
    ) || method.starts_with("catalog_schema")
        || method.starts_with("catalog_table_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_split_into_the_three_classes() {
        assert_eq!(classify_status(200), StatusClass::Success);
        assert_eq!(classify_status(204), StatusClass::Success);
        assert_eq!(classify_status(429), StatusClass::Throttled);
        for s in [502u16, 503, 504] {
            assert_eq!(classify_status(s), StatusClass::Transient, "{s}");
        }
        // A 500 is the worker's own answer and will be the same next time; a
        // 400/404 likewise. None of them may be retried.
        for s in [400u16, 401, 404, 413, 415, 500, 501] {
            assert_eq!(classify_status(s), StatusClass::Fatal, "{s}");
        }
    }

    #[test]
    fn retry_after_reads_both_forms() {
        let now = UNIX_EPOCH + Duration::from_secs(784_111_777);
        assert_eq!(parse_retry_after("2", now), Some(Duration::from_secs(2)));
        assert_eq!(parse_retry_after("  30 ", now), Some(Duration::from_secs(30)));
        assert_eq!(parse_retry_after("0", now), Some(Duration::ZERO));

        // 1994-11-06 08:49:37 GMT is 784_111_777 — the RFC's own example.
        let at = "Sun, 06 Nov 1994 08:49:37 GMT";
        assert_eq!(parse_retry_after(at, now), Some(Duration::ZERO));
        assert_eq!(
            parse_retry_after(at, now - Duration::from_secs(90)),
            Some(Duration::from_secs(90))
        );
        // A date that has already passed is "now", not an error and not a
        // negative wait.
        assert_eq!(
            parse_retry_after(at, now + Duration::from_secs(600)),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn unparseable_retry_after_falls_back_rather_than_failing() {
        let now = SystemTime::now();
        for v in ["", "   ", "soon", "-5", "1.5", "Mon, 32 Xxx 1994 08:49:37 GMT"] {
            assert_eq!(parse_retry_after(v, now), None, "{v:?}");
        }
    }

    #[test]
    fn jitter_spans_the_whole_window_and_adds_to_the_server_floor() {
        let p = RetryPolicy::default();
        // Full jitter: the draw covers [0, window), so two readers that failed
        // together can land a whole window apart.
        assert_eq!(p.delay(1, None, 0.0), Duration::ZERO);
        assert_eq!(p.delay(1, None, 0.999_999).as_millis(), 199);
        // The server's floor is never shortened, only added to.
        let d = p.delay(1, Some(Duration::from_secs(3)), 0.5);
        assert_eq!(d, Duration::from_millis(3_100));
    }

    #[test]
    fn window_doubles_and_then_caps() {
        let p = RetryPolicy::default();
        assert_eq!(p.window(1), Duration::from_millis(200));
        assert_eq!(p.window(2), Duration::from_millis(400));
        assert_eq!(p.window(3), Duration::from_millis(800));
        assert_eq!(p.window(30), p.max_delay);
    }

    #[test]
    fn a_fatal_status_never_sleeps() {
        let p = RetryPolicy::default();
        assert_eq!(
            p.decide(StatusClass::Fatal, 1, Duration::ZERO, None, 0.5),
            RetryDecision::Stop(StopReason::NotRetryable)
        );
        assert_eq!(
            p.decide(StatusClass::Success, 1, Duration::ZERO, None, 0.5),
            RetryDecision::Stop(StopReason::NotRetryable)
        );
    }

    #[test]
    fn attempts_and_time_are_both_capped() {
        let p = RetryPolicy {
            max_attempts: 3,
            max_elapsed: Duration::from_secs(10),
            ..RetryPolicy::default()
        };
        assert!(matches!(
            p.decide(StatusClass::Throttled, 1, Duration::ZERO, None, 0.5),
            RetryDecision::Retry(_)
        ));
        assert_eq!(
            p.decide(StatusClass::Throttled, 3, Duration::ZERO, None, 0.5),
            RetryDecision::Stop(StopReason::AttemptsExhausted)
        );
        // Attempts left, but the sleep would run past the budget.
        assert_eq!(
            p.decide(
                StatusClass::Throttled,
                1,
                Duration::from_secs(9),
                Some(Duration::from_secs(5)),
                0.0
            ),
            RetryDecision::Stop(StopReason::DeadlineExceeded)
        );
        // And a Retry-After longer than the whole budget is refused up front
        // rather than slept through.
        assert_eq!(
            p.decide(
                StatusClass::Throttled,
                1,
                Duration::ZERO,
                Some(Duration::from_secs(3600)),
                0.0
            ),
            RetryDecision::Stop(StopReason::DeadlineExceeded)
        );
    }

    #[test]
    fn giving_up_names_the_endpoint_and_the_attempts() {
        let last = RpcError::new("TransportError", "PUT to upload URL failed: HTTP 503");
        let e = gave_up(
            "init",
            5,
            Duration::from_secs(12),
            StopReason::AttemptsExhausted,
            &last,
        );
        assert!(e.message.contains("init"), "{}", e.message);
        assert!(e.message.contains("5 attempt"), "{}", e.message);
        assert!(e.message.contains("HTTP 503"), "{}", e.message);
    }

    #[test]
    fn statuses_are_recovered_from_the_messages_that_carry_them() {
        assert_eq!(
            status_in_message("fetch external location failed: HTTP 429"),
            Some(429)
        );
        assert_eq!(
            status_in_message("PUT to upload URL failed: HTTP 503"),
            Some(503)
        );
        assert_eq!(status_in_message("empty IPC stream (no schema)"), None);
        assert_eq!(status_in_message("HTTP 42"), None);
    }

    #[test]
    fn errors_classify_conservatively() {
        let (c, after) = classify_error(&RpcError::new("TransportError", "HTTP 429"));
        assert_eq!(c, StatusClass::Throttled);
        assert_eq!(after, None);

        let (c, _) = classify_error(&RpcError::new("TransportError", "HTTP 404"));
        assert_eq!(c, StatusClass::Fatal);

        // The one structured signal the transport does carry.
        let (c, after) = classify_error(&RpcError::auth_unavailable("token store down"));
        assert_eq!(c, StatusClass::Transient);
        assert_eq!(after, Some(Duration::from_secs(5)));

        // A decode failure could be a swallowed 429, but it could equally be a
        // corrupt stream — unplaceable means fatal.
        let (c, _) = classify_error(&RpcError::new("ArrowError", "empty IPC stream (no schema)"));
        assert_eq!(c, StatusClass::Fatal);
    }

    #[test]
    fn only_side_effect_free_methods_are_retried() {
        for m in [
            "bind",
            "table_function_plan",
            "catalog_schemas",
            "catalog_schema_contents_tables",
            "catalog_table_get",
            "catalog_version",
        ] {
            assert!(method_is_retryable(m), "{m}");
        }
        // A duplicate of any of these is a wrong answer or a lost session, not
        // a wasted round trip.
        for m in [
            "catalog_attach",
            "catalog_detach",
            "catalog_transaction_begin",
            "aggregate_update",
            "table_buffering_process",
            "init",
        ] {
            assert!(!method_is_retryable(m), "{m}");
        }
    }

    #[test]
    fn jitter_draws_differ_within_the_unit_interval() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let f = jitter_fraction();
            assert!((0.0..1.0).contains(&f), "{f}");
            seen.insert(f.to_bits());
        }
        // Two readers throttled in the same instant must not compute the same
        // delay; a counter-free hash of the attempt number would fail here.
        assert!(seen.len() > 32, "jitter is not varying: {}", seen.len());
    }

    #[test]
    fn run_retries_then_succeeds_and_counts_attempts() {
        let p = RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            ..RetryPolicy::default()
        };
        let mut calls = 0;
        let out: Result<u8> = p.run("init", || {
            calls += 1;
            if calls < 3 {
                Err(RpcError::new("TransportError", "gateway said HTTP 429"))
            } else {
                Ok(7)
            }
        });
        assert_eq!(out.unwrap(), 7);
        assert_eq!(calls, 3);
    }

    #[test]
    fn run_gives_up_with_a_legible_error() {
        let p = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            ..RetryPolicy::default()
        };
        let mut calls = 0;
        let out: Result<u8> = p.run("table_function_plan", || {
            calls += 1;
            Err(RpcError::new("TransportError", "HTTP 429 rate limited"))
        });
        let e = out.unwrap_err();
        assert_eq!(calls, 3);
        assert!(e.message.contains("table_function_plan"), "{}", e.message);
        assert!(e.message.contains("3 attempt"), "{}", e.message);
    }

    #[test]
    fn run_returns_a_fatal_failure_unchanged() {
        let p = RetryPolicy::default();
        let mut calls = 0;
        let out: Result<u8> = p.run("bind", || {
            calls += 1;
            Err(RpcError::value_error("no such function"))
        });
        let e = out.unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(e.error_type, "ValueError");
        assert_eq!(e.message, "no such function");
    }
}

// ---------------------------------------------------------------------------

/// A [`VgiTransport`] that applies a [`RetryPolicy`] to the calls it can.
///
/// Wrapped around the HTTP transports only. A byte-stream worker (subprocess,
/// AF_UNIX, TCP) has no status codes and no rate limiter in front of it, so
/// there is nothing here for it to classify.
///
/// # What is retried, and what is left alone
///
/// Unary calls whose method is on [`method_is_retryable`]'s allowlist. That
/// covers the two calls a distributed engine makes most and cares about most —
/// `bind` and `table_function_plan`, the latter being split enumeration, which
/// is exactly where an over-fanning engine meets the cap.
///
/// The stream opens (`init`) are forwarded with a single attempt, and the
/// reason is a borrow, not a judgement: the session they return borrows the
/// client for as long as the caller holds it, so a loop that re-opened after a
/// failure would be re-borrowing across iterations — rejected before Polonius.
/// A caller that owns its client retries those itself through
/// [`RetryPolicy::run`], which is why the policy is a value and not private
/// state.
pub struct RetryTransport {
    inner: Box<dyn VgiTransport>,
    policy: RetryPolicy,
}

impl RetryTransport {
    /// Wrap `inner`, applying `policy` to the calls that can carry it.
    pub fn new(inner: Box<dyn VgiTransport>, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }

    /// The policy in force, for a caller driving its own retries around the
    /// stream opens this cannot cover.
    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }
}

impl VgiTransport for RetryTransport {
    fn call_unary(&mut self, method: &str, params: &RecordBatch) -> Result<RecordBatch> {
        if !method_is_retryable(method) {
            return self.inner.call_unary(method, params);
        }
        let policy = self.policy;
        let inner = &mut self.inner;
        policy.run(method, || inner.call_unary(method, params))
    }

    fn open_producer<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        has_header: bool,
    ) -> Result<Box<dyn ProducerStream + 'a>> {
        self.inner.open_producer(method, params, has_header)
    }

    fn open_exchange<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        has_header: bool,
    ) -> Result<Box<dyn ExchangeStream + 'a>> {
        self.inner.open_exchange(method, params, has_header)
    }

    fn label(&self) -> &str {
        self.inner.label()
    }
}
