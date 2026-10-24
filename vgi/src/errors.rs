// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Coded errors: tell "your input was wrong" apart from "the worker broke".
//!
//! Every error a worker returns travels as a vgi-rpc EXCEPTION batch, and every
//! such batch carries a gRPC-style canonical code in `vgi_rpc.error_code`
//! (vgi-rpc `WIRE_PROTOCOL.md` §8, "Error model"). An error built with plain
//! [`RpcError::value_error`] / [`RpcError::runtime_error`] carries no code and
//! goes out as `UNKNOWN` -- the right answer for a bug, the wrong one for a
//! caller who passed a bad argument. The DuckDB extension (and
//! `SET errors_as_json = true`) reads the code to make that distinction.
//!
//! The constructors here attach the code for the four situations the SDK
//! classifies itself, and are meant for worker code too:
//!
//! | Constructor              | Code                  | Use for                                                       |
//! |--------------------------|-----------------------|---------------------------------------------------------------|
//! | [`invalid_argument`]     | `INVALID_ARGUMENT`    | A bad argument value, or an argument / input column of the wrong type |
//! | [`not_found`]            | `NOT_FOUND`           | An unknown function, table, schema, catalog or other object   |
//! | [`failed_precondition`]  | `FAILED_PRECONDITION` | A write against a read-only catalog                           |
//! | [`unimplemented()`]      | `UNIMPLEMENTED`       | An operation this worker explicitly does not support          |
//!
//! Anything else -- a worker bug, an output that breaks its own declared
//! schema, a panic -- should stay uncoded (`UNKNOWN`). The message is free
//! text for a developer; the code is the contract.
//!
//! ```
//! use vgi::errors;
//!
//! fn check_batch_size(batch_size: i64) -> vgi::Result<()> {
//!     if batch_size < 1 {
//!         return Err(errors::invalid_argument("batch_size must be >= 1"));
//!     }
//!     Ok(())
//! }
//!
//! let err = check_batch_size(0).unwrap_err();
//! assert_eq!(err.error_code(), "INVALID_ARGUMENT");
//! assert_eq!(err.code(), errors::Code::InvalidArgument);
//! ```
//!
//! To code an error built some other way, use [`RpcError::with_code`]:
//! `RpcError::type_error(msg).with_code(Code::InvalidArgument)`.

pub use vgi_rpc::error_model::Code;
use vgi_rpc::RpcError;

/// A bad argument: a value outside its allowed range or set, or an argument /
/// input column whose type the function does not accept. Code
/// `INVALID_ARGUMENT`; `error_type` `ValueError`.
pub fn invalid_argument(msg: impl Into<String>) -> RpcError {
    RpcError::value_error(msg).with_code(Code::InvalidArgument)
}

/// A lookup that found nothing: an unknown function, table, schema, catalog or
/// version. Code `NOT_FOUND`; `error_type` `ValueError`.
pub fn not_found(msg: impl Into<String>) -> RpcError {
    RpcError::value_error(msg).with_code(Code::NotFound)
}

/// The system is not in a state that allows the operation -- in the SDK, a
/// write against a read-only catalog. Code `FAILED_PRECONDITION`;
/// `error_type` `RuntimeError`.
pub fn failed_precondition(msg: impl Into<String>) -> RpcError {
    RpcError::runtime_error(msg).with_code(Code::FailedPrecondition)
}

/// An operation this worker explicitly does not support. Code
/// `UNIMPLEMENTED`; `error_type` `RuntimeError`.
pub fn unimplemented(msg: impl Into<String>) -> RpcError {
    RpcError::runtime_error(msg).with_code(Code::Unimplemented)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_carry_their_codes() {
        let cases = [
            (invalid_argument("x"), "INVALID_ARGUMENT", "ValueError"),
            (not_found("x"), "NOT_FOUND", "ValueError"),
            (
                failed_precondition("x"),
                "FAILED_PRECONDITION",
                "RuntimeError",
            ),
            (unimplemented("x"), "UNIMPLEMENTED", "RuntimeError"),
        ];
        for (err, code, ty) in cases {
            assert_eq!(err.error_code(), code);
            assert_eq!(err.error_type, ty);
            assert_eq!(err.message, "x");
            assert_eq!(err.error_kind(), "", "no error_kind is invented");
        }
    }
}
