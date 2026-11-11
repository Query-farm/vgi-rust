// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Dict-encoded enum string values used across the VGI wire protocol.
//!
//! Each is serialized as `dictionary(int16, utf8)` (see `vgi_rpc::DictString`);
//! these constants are the canonical string payloads from Python
//! `vgi/metadata.py`, `vgi/catalog/catalog_interface.py`, `vgi/protocol.py`
//! and `vgi/table_function.py` — always the enum **member name**, which is
//! what `vgi_rpc` puts on the wire for an `Enum` field.

use vgi_rpc::DictString;

/// Build a `DictString` from a `&str`.
pub fn dict(s: &str) -> DictString {
    DictString(s.to_string())
}

/// `TableInfo.scan_planning` (protocol 3.2.0) — when the client resolves a
/// table's scan redirect. Unlike the modules around it this is a plain `utf8`
/// column, not a dictionary-encoded enum, and its values are lower case.
pub mod scan_planning {
    /// Resolve the scan redirect at bind time (protocol 3.1 behaviour); the
    /// default.
    pub const BIND: &str = "bind";
    /// Resolve it once per scan, after the client has collected the query's
    /// predicates on the table, which it sends as `pushdown_filters`.
    pub const FILTERED: &str = "filtered";

    /// Reject any value other than [`BIND`] and [`FILTERED`].
    pub fn validate(value: &str) -> vgi_rpc::Result<()> {
        if value == BIND || value == FILTERED {
            Ok(())
        } else {
            Err(vgi_rpc::RpcError::value_error(format!(
                "scan_planning must be '{BIND}' or '{FILTERED}', got {value:?}"
            )))
        }
    }
}

/// `FunctionInfo.visibility` (protocol 3.3.0) — who can see a listed function.
/// A plain nullable `utf8` column like [`scan_planning`], not a dictionary;
/// null means [`PUBLIC`](function_visibility::PUBLIC).
pub mod function_visibility {
    /// An ordinary catalog entry: in `duckdb_functions()`, callable from SQL.
    pub const PUBLIC: &str = "public";
    /// Listed only so the client has the function's capabilities when it
    /// resolves a table's scan or write function; absent from
    /// `duckdb_functions()` and not callable from SQL.
    pub const TABLE_ONLY: &str = "table_only";

    /// Reject any value other than [`PUBLIC`] and [`TABLE_ONLY`] (`None` is
    /// public).
    pub fn validate(value: Option<&str>) -> vgi_rpc::Result<()> {
        match value {
            None | Some(PUBLIC) | Some(TABLE_ONLY) => Ok(()),
            Some(other) => Err(vgi_rpc::RpcError::value_error(format!(
                "visibility must be '{PUBLIC}' or '{TABLE_ONLY}', got {other:?}"
            ))),
        }
    }
}

/// `FunctionType` — which registry a function lives in (`FunctionInfo` and
/// `BindRequest.function_type`).
///
/// Every dictionary-encoded enum column carries the Python enum **member
/// name** (`.name`), never its `.value`: vgi-python's `FunctionType.SCALAR` has
/// the value `"scalar"`, but it crosses the wire as `"SCALAR"`. The C++ vgi
/// extension decodes these columns with strict generated codecs that reject the
/// lower-case value. There is no `TABLE_IN_OUT` member: a streaming
/// table-in-out function is a `TABLE` function.
pub mod function_type {
    pub const SCALAR: &str = "SCALAR";
    pub const TABLE: &str = "TABLE";
    pub const TABLE_BUFFERING: &str = "TABLE_BUFFERING";
    pub const AGGREGATE: &str = "AGGREGATE";
}

/// `MacroType` — `MacroInfo.macro_type` / `MacroCreateRequest.macro_type`.
/// Member names, like [`function_type`].
pub mod macro_type {
    pub const SCALAR: &str = "SCALAR";
    pub const TABLE: &str = "TABLE";
}

/// `IndexConstraintType` — `IndexInfo.constraint_type` /
/// `IndexCreateRequest.constraint_type`.
pub mod index_constraint_type {
    pub const NONE: &str = "NONE";
    pub const UNIQUE: &str = "UNIQUE";
    pub const PRIMARY: &str = "PRIMARY";
}

/// `OnConflict` — the `on_conflict` column of the DDL requests.
pub mod on_conflict {
    pub const ERROR: &str = "ERROR";
    pub const IGNORE: &str = "IGNORE";
    pub const REPLACE: &str = "REPLACE";
}

/// `OrderByDirection` — `InitRequest.order_by_direction`,
/// `TableFunctionPlanRequest.order_by_direction` and `SortField.direction`.
pub mod order_by_direction {
    pub const ASC: &str = "ASC";
    pub const DESC: &str = "DESC";
}

/// `OrderByNullOrder` — `InitRequest.order_by_null_order`,
/// `TableFunctionPlanRequest.order_by_null_order` and `SortField.nulls`.
pub mod order_by_null_order {
    pub const NULLS_FIRST: &str = "NULLS_FIRST";
    pub const NULLS_LAST: &str = "NULLS_LAST";
}

/// `FunctionStability`.
pub mod stability {
    pub const CONSISTENT: &str = "CONSISTENT";
    pub const VOLATILE: &str = "VOLATILE";
    pub const CONSISTENT_WITHIN_QUERY: &str = "CONSISTENT_WITHIN_QUERY";
}

/// `NullHandling`.
///
/// Wire values are the Python `NullHandling` enum member names (`.name`), which
/// the C++ vgi extension's `ParseFunctionNullHandling` matches exactly. They
/// MUST be uppercase — lowercase values are silently ignored by the extension
/// (falling back to DEFAULT), which notably breaks scalar functions that rely
/// on SPECIAL handling to receive NULL arguments.
pub mod null_handling {
    pub const DEFAULT: &str = "DEFAULT";
    pub const SPECIAL: &str = "SPECIAL";
}

/// `OrderPreservation`.
pub mod order_preservation {
    pub const PRESERVES_ORDER: &str = "PRESERVES_ORDER";
    pub const NO_ORDER_GUARANTEE: &str = "NO_ORDER_GUARANTEE";
    pub const FIXED_ORDER: &str = "FIXED_ORDER";
}

/// `PartitionKind`.
pub mod partition_kind {
    pub const NOT_PARTITIONED: &str = "NOT_PARTITIONED";
    pub const SINGLE_VALUE_PARTITIONS: &str = "SINGLE_VALUE_PARTITIONS";
    pub const OVERLAPPING_PARTITIONS: &str = "OVERLAPPING_PARTITIONS";
    pub const DISJOINT_PARTITIONS: &str = "DISJOINT_PARTITIONS";
}

/// `OrderDependence`.
///
/// Wire values are the Python `OrderDependence` enum member names (`.name`),
/// which the C++ vgi extension's `ParseAggregateOrderDependent` matches exactly
/// (via `RequireKnownEnum`). They MUST be uppercase — a lowercase value is
/// rejected outright, failing catalog load for every function. See the sibling
/// note on [`null_handling`].
pub mod order_dependence {
    pub const ORDER_DEPENDENT: &str = "ORDER_DEPENDENT";
    pub const NOT_ORDER_DEPENDENT: &str = "NOT_ORDER_DEPENDENT";
}

/// `DistinctDependence`.
///
/// Wire values are the Python `DistinctDependence` enum member names (`.name`),
/// matched exactly by the C++ `ParseAggregateDistinctDependent` via
/// `RequireKnownEnum`. They MUST be uppercase — see [`order_dependence`].
pub mod distinct_dependence {
    pub const DISTINCT_DEPENDENT: &str = "DISTINCT_DEPENDENT";
    pub const NOT_DISTINCT_DEPENDENT: &str = "NOT_DISTINCT_DEPENDENT";
}

/// `TableInOutFunctionInitPhase` — the `init` request `phase` enum.
pub mod phase {
    pub const PROCESS: &str = "PROCESS";
    pub const INPUT: &str = "INPUT";
    pub const FINALIZE: &str = "FINALIZE";
    pub const TABLE_BUFFERING: &str = "TABLE_BUFFERING";
    pub const TABLE_BUFFERING_FINALIZE: &str = "TABLE_BUFFERING_FINALIZE";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_visibility_accepts_only_public_and_table_only() {
        assert!(function_visibility::validate(None).is_ok());
        assert!(function_visibility::validate(Some("public")).is_ok());
        assert!(function_visibility::validate(Some("table_only")).is_ok());
        for bad in ["", "hidden", "TABLE_ONLY", "Public"] {
            assert!(function_visibility::validate(Some(bad)).is_err(), "{bad:?}");
        }
    }

    /// Each constant set is exactly the member names of its vgi-python enum, in
    /// upper case — what `vgi_rpc` writes for an `Enum` field and what the
    /// DuckDB extension's strict generated decoders accept.
    #[test]
    fn constants_are_python_member_names() {
        let sets: [(&str, &[&str], &[&str]); 12] = [
            (
                "FunctionType",
                &[
                    function_type::SCALAR,
                    function_type::TABLE,
                    function_type::TABLE_BUFFERING,
                    function_type::AGGREGATE,
                ],
                &["SCALAR", "TABLE", "TABLE_BUFFERING", "AGGREGATE"],
            ),
            (
                "MacroType",
                &[macro_type::SCALAR, macro_type::TABLE],
                &["SCALAR", "TABLE"],
            ),
            (
                "IndexConstraintType",
                &[
                    index_constraint_type::NONE,
                    index_constraint_type::UNIQUE,
                    index_constraint_type::PRIMARY,
                ],
                &["NONE", "UNIQUE", "PRIMARY"],
            ),
            (
                "OnConflict",
                &[
                    on_conflict::ERROR,
                    on_conflict::IGNORE,
                    on_conflict::REPLACE,
                ],
                &["ERROR", "IGNORE", "REPLACE"],
            ),
            (
                "OrderByDirection",
                &[order_by_direction::ASC, order_by_direction::DESC],
                &["ASC", "DESC"],
            ),
            (
                "OrderByNullOrder",
                &[
                    order_by_null_order::NULLS_FIRST,
                    order_by_null_order::NULLS_LAST,
                ],
                &["NULLS_FIRST", "NULLS_LAST"],
            ),
            (
                "FunctionStability",
                &[
                    stability::CONSISTENT,
                    stability::VOLATILE,
                    stability::CONSISTENT_WITHIN_QUERY,
                ],
                &["CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"],
            ),
            (
                "NullHandling",
                &[null_handling::DEFAULT, null_handling::SPECIAL],
                &["DEFAULT", "SPECIAL"],
            ),
            (
                "OrderPreservation",
                &[
                    order_preservation::PRESERVES_ORDER,
                    order_preservation::NO_ORDER_GUARANTEE,
                    order_preservation::FIXED_ORDER,
                ],
                &["PRESERVES_ORDER", "NO_ORDER_GUARANTEE", "FIXED_ORDER"],
            ),
            (
                "PartitionKind",
                &[
                    partition_kind::NOT_PARTITIONED,
                    partition_kind::SINGLE_VALUE_PARTITIONS,
                    partition_kind::OVERLAPPING_PARTITIONS,
                    partition_kind::DISJOINT_PARTITIONS,
                ],
                &[
                    "NOT_PARTITIONED",
                    "SINGLE_VALUE_PARTITIONS",
                    "OVERLAPPING_PARTITIONS",
                    "DISJOINT_PARTITIONS",
                ],
            ),
            (
                "OrderDependence",
                &[
                    order_dependence::ORDER_DEPENDENT,
                    order_dependence::NOT_ORDER_DEPENDENT,
                ],
                &["ORDER_DEPENDENT", "NOT_ORDER_DEPENDENT"],
            ),
            (
                "DistinctDependence",
                &[
                    distinct_dependence::DISTINCT_DEPENDENT,
                    distinct_dependence::NOT_DISTINCT_DEPENDENT,
                ],
                &["DISTINCT_DEPENDENT", "NOT_DISTINCT_DEPENDENT"],
            ),
        ];
        for (name, consts, members) in sets {
            assert_eq!(consts, members, "{name}");
        }
    }
}
