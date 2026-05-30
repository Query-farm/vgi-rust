//! Dict-encoded enum string values used across the VGI wire protocol.
//!
//! Each is serialized as `dictionary(int16, utf8)` (see `vgi_rpc::DictString`);
//! these constants are the canonical string payloads from Python
//! `vgi/metadata.py` and `vgi/catalog/catalog_interface.py`.

use vgi_rpc::DictString;

/// Build a `DictString` from a `&str`.
pub fn dict(s: &str) -> DictString {
    DictString(s.to_string())
}

/// `FunctionType` — which registry a function lives in.
pub mod function_type {
    pub const SCALAR: &str = "scalar";
    pub const TABLE: &str = "table";
    pub const TABLE_BUFFERING: &str = "table_buffering";
    pub const AGGREGATE: &str = "aggregate";
}

/// `FunctionStability`.
pub mod stability {
    pub const CONSISTENT: &str = "CONSISTENT";
    pub const VOLATILE: &str = "VOLATILE";
    pub const CONSISTENT_WITHIN_QUERY: &str = "CONSISTENT_WITHIN_QUERY";
}

/// `NullHandling`.
pub mod null_handling {
    pub const DEFAULT: &str = "default";
    pub const SPECIAL: &str = "special";
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
pub mod order_dependence {
    pub const ORDER_DEPENDENT: &str = "order_dependent";
    pub const NOT_ORDER_DEPENDENT: &str = "not_order_dependent";
}

/// `DistinctDependence`.
pub mod distinct_dependence {
    pub const DISTINCT_DEPENDENT: &str = "distinct_dependent";
    pub const NOT_DISTINCT_DEPENDENT: &str = "not_distinct_dependent";
}

/// `TableInOutFunctionInitPhase` — the `init` request `phase` enum.
pub mod phase {
    pub const PROCESS: &str = "PROCESS";
    pub const TABLE_BUFFERING: &str = "TABLE_BUFFERING";
    pub const TABLE_BUFFERING_FINALIZE: &str = "TABLE_BUFFERING_FINALIZE";
}
