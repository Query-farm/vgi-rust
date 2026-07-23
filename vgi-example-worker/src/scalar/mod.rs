// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Scalar example fixtures.

mod cached;
mod fmt;
mod geo;
pub mod same_name;
pub mod util;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    StructArray,
};
use arrow_schema::DataType;
use sha2::{Digest, Sha256};
use util::*;
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionExample, FunctionMetadata, ProcessParams,
    ScalarFunction, ADDABLE, MULTIPLIABLE,
};
use vgi::numeric::{add_two, common_type_for_addition, double_first, promote_for_addition};
use vgi::secrets::SecretLookup;
use vgi_rpc::{Result, RpcError};

const HEX_LUT: &[u8; 16] = b"0123456789abcdef";

#[inline]
fn hex_of(bytes: &[u8]) -> String {
    // Lookup-table hex — the previous `format!("{:02x}")` per byte cost ~1 µs/row
    // (32 allocating format! calls), which swamped the hash itself. Matches the
    // Go/Java fixtures' fast path.
    let mut out = vec![0u8; bytes.len() * 2];
    for (i, &b) in bytes.iter().enumerate() {
        out[i * 2] = HEX_LUT[(b >> 4) as usize];
        out[i * 2 + 1] = HEX_LUT[(b & 0x0f) as usize];
    }
    // Safety: HEX_LUT bytes are ASCII, so `out` is valid UTF-8.
    unsafe { String::from_utf8_unchecked(out) }
}

/// Register all scalar fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_scalar(DoubleFunction);
    w.register_scalar(AddValuesFunction);
    w.register_scalar(MultiplyFunction);
    w.register_scalar(PassthruFunction);
    w.register_scalar(CollatzStepsFunction);
    w.register_scalar(Sha256HexFunction);
    w.register_scalar(HashRoundsFunction);
    w.register_scalar(SumValuesFunction);
    w.register_scalar(ConcatValuesIntFunction);
    w.register_scalar(ConcatValuesStrFunction);
    w.register_scalar(UpperCaseFunction);
    w.register_scalar(NullHandlingFunction);
    w.register_scalar(ConditionalMessageFunction);
    w.register_scalar(HashSeedFunction);
    w.register_scalar(QuerySeedFunction);
    w.register_scalar(BernoulliFunction);
    w.register_scalar(RandomIntFunction);
    w.register_scalar(RandomBytesFunction);
    w.register_scalar(BinaryPacketFunction);
    w.register_scalar(MultiplyBySettingFunction);
    w.register_scalar(ScaleBySettingFunction);
    w.register_scalar(ReturnSecretValueFunction);
    w.register_scalar(SecretFieldFunction);
    w.register_scalar(WhoAmIFunction);
    // type_info overloads
    w.register_scalar(TypeInfo("int32", DataType::Int32));
    w.register_scalar(TypeInfo("int64", DataType::Int64));
    w.register_scalar(TypeInfo("uint32", DataType::UInt32));
    w.register_scalar(TypeInfo("uint64", DataType::UInt64));
    w.register_scalar(TypeInfo("varchar", DataType::Utf8));
    // pair_type overloads
    w.register_scalar(PairType("int+int", DataType::Int64, DataType::Int64));
    w.register_scalar(PairType("str+str", DataType::Utf8, DataType::Utf8));
    w.register_scalar(PairType("int+str", DataType::Int64, DataType::Utf8));
    // any_mixed overloads
    w.register_scalar(AnyMixed("int", DataType::Int64));
    w.register_scalar(AnyMixed("str", DataType::Utf8));
    geo::register(w);
    fmt::register(w);
    cached::register(w);
}

fn meta(desc: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        ..Default::default()
    }
}
fn meta_ret(desc: &str, ret: DataType) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        return_type: Some(ret),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// arithmetic
// ---------------------------------------------------------------------------

/// `double(value)` — doubles numeric values.
pub struct DoubleFunction;
impl ScalarFunction for DoubleFunction {
    fn name(&self) -> &str {
        "double"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            examples: vec![
                FunctionExample {
                    sql: "SELECT double(21)".to_string(),
                    description: "Double an integer literal".to_string(),
                    expected_output: Some("42".to_string()),
                },
                FunctionExample {
                    sql: "SELECT double(value) FROM numbers".to_string(),
                    description: "Double every value in a column".to_string(),
                    expected_output: None,
                },
            ],
            ..meta("Doubles numeric values")
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::any_column("value", 0, "Numeric value to double").with_bound(MULTIPLIABLE)]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(promote_for_addition(
            &first_input_type(params),
        )))
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        double_first(params, batch)
    }
}

/// `add_values(col1, col2)` — adds two numeric values.
pub struct AddValuesFunction;
impl ScalarFunction for AddValuesFunction {
    fn name(&self) -> &str {
        "add_values"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Adds two numeric values")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::any_column("col1", 0, "First numeric value").with_bound(ADDABLE),
            ArgSpec::any_column("col2", 1, "Second numeric value").with_bound(ADDABLE),
        ]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(common_type_for_addition(
            &nth_input_type(params, 0),
            &nth_input_type(params, 1),
        )))
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        add_two(params, batch)
    }
}

/// `multiply(value, factor_const)` — value * constant factor.
pub struct MultiplyFunction;
impl ScalarFunction for MultiplyFunction {
    fn name(&self) -> &str {
        "multiply"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Multiplies a value by a constant factor", DataType::Int64)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("value", 0, "int64", "Integer value to multiply"),
            ArgSpec::const_arg("factor", 1, "int64", "Multiplication factor"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let factor = params.arguments.const_i64(1).unwrap_or(1);
        let v = arrow_cast::cast(batch.column(0), &DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let a = v.as_primitive::<arrow_array::types::Int64Type>();
        let out: Int64Array = (0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i) * factor))
            .collect();
        result(params, arc(out))
    }
}

/// `passthru(s)` — identity: return the input string unchanged. Zero compute,
/// so a payload sweep over it measures pure round-trip wire cost per byte.
pub struct PassthruFunction;
impl ScalarFunction for PassthruFunction {
    fn name(&self) -> &str {
        "passthru"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Returns the input string unchanged (zero-compute wire probe)",
            DataType::Utf8,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "varchar", "String value")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        result(params, batch.column(0).clone())
    }
}

/// `collatz_steps(n)` — number of Collatz (3n+1) steps to reach 1. CPU-bound,
/// data-dependent per-row loop (the compute-ladder anchor).
pub struct CollatzStepsFunction;
impl ScalarFunction for CollatzStepsFunction {
    fn name(&self) -> &str {
        "collatz_steps"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Number of Collatz (3n+1) steps to reach 1", DataType::Int64)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Positive integer")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let v = arrow_cast::cast(batch.column(0), &DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let a = v.as_primitive::<arrow_array::types::Int64Type>();
        let out: Int64Array = (0..a.len())
            .map(|i| {
                if a.is_null(i) {
                    return None;
                }
                let mut n = a.value(i) as i128; // i128 guards the 3n+1 spikes
                if n <= 0 {
                    return Some(0);
                }
                let mut steps: i64 = 0;
                while n != 1 {
                    n = if n & 1 == 0 { n / 2 } else { 3 * n + 1 };
                    steps += 1;
                }
                Some(steps)
            })
            .collect();
        result(params, arc(out))
    }
}

/// `sha256_hex(s)` — lowercase hex SHA-256 of the UTF-8 string. Fixed compute/byte.
pub struct Sha256HexFunction;
impl ScalarFunction for Sha256HexFunction {
    fn name(&self) -> &str {
        "sha256_hex"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Lowercase hex SHA-256 of the UTF-8 string", DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "varchar", "String to hash")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let s = batch.column(0).as_string::<i32>();
        let out: StringArray = (0..s.len())
            .map(|i| {
                (!s.is_null(i)).then(|| {
                    let mut h = Sha256::new();
                    h.update(s.value(i).as_bytes());
                    hex_of(&h.finalize())
                })
            })
            .collect();
        result(params, arc(out))
    }
}

/// `hash_rounds(s, rounds_const)` — apply SHA-256 `rounds` times (key-stretching).
/// `rounds` is the const compute knob at fixed payload (the compute-sweep function).
pub struct HashRoundsFunction;
impl ScalarFunction for HashRoundsFunction {
    fn name(&self) -> &str {
        "hash_rounds"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Apply SHA-256 `rounds` times (key-stretching); rounds is a const compute knob",
            DataType::Utf8,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("value", 0, "varchar", "String to stretch"),
            ArgSpec::const_arg("rounds", 1, "int64", "Number of SHA-256 rounds"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let k = params.arguments.const_i64(1).unwrap_or(1).max(0) as usize;
        let s = batch.column(0).as_string::<i32>();
        let out: StringArray = (0..s.len())
            .map(|i| {
                (!s.is_null(i)).then(|| {
                    let mut buf = s.value(i).as_bytes().to_vec();
                    for _ in 0..k {
                        let mut h = Sha256::new();
                        h.update(&buf);
                        buf = h.finalize().to_vec();
                    }
                    hex_of(&buf)
                })
            })
            .collect();
        result(params, arc(out))
    }
}

/// `sum_values(...)` — varargs addable, promoted output.
pub struct SumValuesFunction;
impl ScalarFunction for SumValuesFunction {
    fn name(&self) -> &str {
        "sum_values"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Sum multiple numeric values")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::any_column("values", 0, "Numeric values to sum")
            .varargs()
            .with_bound(ADDABLE)]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(promote_for_addition(
            &first_input_type(params),
        )))
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.num_columns() == 0 {
            return Err(RpcError::value_error("requires at least 1 value"));
        }
        let ty = params.output_schema.field(0).data_type().clone();
        let mut acc = arrow_cast::cast(batch.column(0), &ty)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        for i in 1..batch.num_columns() {
            let c = arrow_cast::cast(batch.column(i), &ty)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
            acc = arrow_arith::numeric::add(&acc, &c)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        }
        let acc =
            arrow_cast::cast(&acc, &ty).map_err(|e| RpcError::runtime_error(e.to_string()))?;
        result(params, acc)
    }
}

/// `concat_values(ints...)` — sum int varargs, render as string.
pub struct ConcatValuesIntFunction;
impl ScalarFunction for ConcatValuesIntFunction {
    fn name(&self) -> &str {
        "concat_values"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Sum integer varargs and return as string", DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("values", 0, "int64", "Integer values to sum").varargs()]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut acc = arrow_cast::cast(batch.column(0), &DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        for i in 1..batch.num_columns() {
            let c = arrow_cast::cast(batch.column(i), &DataType::Int64)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
            acc = arrow_arith::numeric::add(&acc, &c)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        }
        let s = arrow_cast::cast(&acc, &DataType::Utf8)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        result(params, s)
    }
}

/// `concat_values(strs...)` — concatenate string varargs.
pub struct ConcatValuesStrFunction;
impl ScalarFunction for ConcatValuesStrFunction {
    fn name(&self) -> &str {
        "concat_values"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Concatenate string varargs", DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("values", 0, "varchar", "String values to concatenate").varargs()]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let n = batch.num_rows();
        let cols: Vec<&StringArray> = (0..batch.num_columns())
            .map(|i| batch.column(i).as_string::<i32>())
            .collect();
        let out: StringArray = (0..n)
            .map(|r| {
                if cols.iter().any(|c| c.is_null(r)) {
                    None
                } else {
                    Some(cols.iter().map(|c| c.value(r)).collect::<String>())
                }
            })
            .collect();
        result(params, arc(out))
    }
}

// ---------------------------------------------------------------------------
// strings / binary
// ---------------------------------------------------------------------------

/// `upper_case(s)` — uppercase strings.
pub struct UpperCaseFunction;
impl ScalarFunction for UpperCaseFunction {
    fn name(&self) -> &str {
        "upper_case"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Converts string values to uppercase", DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "value",
            0,
            "varchar",
            "String value to uppercase",
        )]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let s = batch.column(0).as_string::<i32>();
        let out: StringArray = (0..s.len())
            .map(|i| (!s.is_null(i)).then(|| s.value(i).to_uppercase()))
            .collect();
        result(params, arc(out))
    }
}

/// `binary_packet(header_const, payload, config_const)` — header+payload+suffix.
pub struct BinaryPacketFunction;
impl ScalarFunction for BinaryPacketFunction {
    fn name(&self) -> &str {
        "binary_packet"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Build binary packets with header, payload, and config",
            DataType::Binary,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        let config_ty = DataType::Struct(
            vec![
                arrow_schema::Field::new("label", DataType::Utf8, true),
                arrow_schema::Field::new("version", DataType::Int64, true),
            ]
            .into(),
        );
        vec![
            ArgSpec::const_typed("header", 0, DataType::Binary, "Header bytes to prepend"),
            ArgSpec::column("payload", 1, "binary", "Binary payload data"),
            ArgSpec::const_typed("config", 2, config_ty, "Config {label, version}"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let header = params.arguments.const_bytes(0).unwrap_or_default();
        let (label, version) = match params.arguments.arg(2) {
            Some(a) => {
                let sa = a
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| RpcError::runtime_error("config not a struct"))?;
                let label = sa
                    .column_by_name("label")
                    .and_then(|c| c.as_string_opt::<i32>().map(|s| s.value(0).to_string()))
                    .unwrap_or_default();
                let version = sa
                    .column_by_name("version")
                    .and_then(|c| {
                        vgi::numeric::array_value_i64(&arrow_array::make_array(c.to_data()), 0)
                    })
                    .unwrap_or(0);
                (label, version)
            }
            None => (String::new(), 0),
        };
        let mut suffix = label.into_bytes();
        suffix.push((version & 0xFF) as u8);

        let payload = batch.column(0).as_binary::<i32>();
        let out: BinaryArray = (0..payload.len())
            .map(|i| {
                let mut v = header.clone();
                if !payload.is_null(i) {
                    v.extend_from_slice(payload.value(i));
                }
                v.extend_from_slice(&suffix);
                Some(v)
            })
            .collect();
        result(params, arc(out))
    }
}

// ---------------------------------------------------------------------------
// null handling / conditional
// ---------------------------------------------------------------------------

/// `null_handling(value)` — value or -5000 when null (SPECIAL null handling).
pub struct NullHandlingFunction;
impl ScalarFunction for NullHandlingFunction {
    fn name(&self) -> &str {
        "null_handling"
    }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = meta_ret("Returns value or -5000 if null", DataType::Int64);
        m.null_handling = Some(vgi::protocol::enums::null_handling::SPECIAL.to_string());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "value",
            0,
            "int64",
            "Integer value to process",
        )]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let a = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let out: Int64Array = (0..a.len())
            .map(|i| Some(if a.is_null(i) { -5000 } else { a.value(i) }))
            .collect();
        result(params, arc(out))
    }
}

/// `conditional_message(repeat_const, msg_const, condition)` → repeated message.
pub struct ConditionalMessageFunction;
impl ScalarFunction for ConditionalMessageFunction {
    fn name(&self) -> &str {
        "conditional_message"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Returns repeated message when condition is true",
            DataType::Utf8,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("repeat_count", 0, "int64", "Number of times to repeat"),
            ArgSpec::const_arg("message", 1, "varchar", "Message to repeat"),
            ArgSpec::column("condition", 2, "bool", "Apply message condition"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let rc = params.arguments.const_i64(0).unwrap_or(0).max(0) as usize;
        let msg = params.arguments.const_str(1).unwrap_or_default();
        let repeated = msg.repeat(rc);
        let cond = batch.column(0).as_boolean();
        let out: StringArray = (0..cond.len())
            .map(|i| {
                Some(if !cond.is_null(i) && cond.value(i) {
                    repeated.clone()
                } else {
                    String::new()
                })
            })
            .collect();
        result(params, arc(out))
    }
}

// ---------------------------------------------------------------------------
// random / seeded
// ---------------------------------------------------------------------------

/// `hash_seed(seed_const)` — seed + row_index per row.
pub struct HashSeedFunction;
impl ScalarFunction for HashSeedFunction {
    fn name(&self) -> &str {
        "hash_seed"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Generate deterministic integers from a constant seed",
            DataType::Int64,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("seed", 0, "int64", "Seed value")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let seed = params.arguments.const_i64(0).unwrap_or(0);
        let n = output_len(batch);
        let out: Int64Array = (0..n as i64).map(|i| Some(seed + i)).collect();
        result(params, arc(out))
    }
}

/// `query_seed(value)` — adds a per-query-stable seed (CONSISTENT_WITHIN_QUERY).
///
/// The only fixture emitting `CONSISTENT_WITHIN_QUERY`; the offset is a fixed
/// constant here so SQL tests have a stable expected output — the stability
/// flag is what's under test, not the numeric result.
pub struct QuerySeedFunction;
impl ScalarFunction for QuerySeedFunction {
    fn name(&self) -> &str {
        "query_seed"
    }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = meta_ret(
            "Add a per-query-stable seed to each value (demonstrates CONSISTENT_WITHIN_QUERY stability)",
            DataType::Int64,
        );
        m.stability = Some(vgi::protocol::enums::stability::CONSISTENT_WITHIN_QUERY.to_string());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Value to offset")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let value = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let out: Int64Array = (0..value.len())
            .map(|i| (!value.is_null(i)).then(|| value.value(i) + 1000))
            .collect();
        result(params, arc(out))
    }
}

/// `bernoulli()` — random booleans (VOLATILE).
pub struct BernoulliFunction;
impl ScalarFunction for BernoulliFunction {
    fn name(&self) -> &str {
        "bernoulli"
    }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = meta_ret(
            "Generate random booleans (demonstrates VOLATILE stability)",
            DataType::Boolean,
        );
        m.stability = Some(vgi::protocol::enums::stability::VOLATILE.to_string());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let n = output_len(batch);
        let mut rng = Rng::new(volatile_seed());
        let out: BooleanArray = (0..n).map(|_| Some(rng.next_u64() & 1 == 1)).collect();
        result(params, arc(out))
    }
}

/// `random_int(min, max)` — random int per row (VOLATILE).
pub struct RandomIntFunction;
impl ScalarFunction for RandomIntFunction {
    fn name(&self) -> &str {
        "random_int"
    }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = meta_ret(
            "Generate random integers (demonstrates VOLATILE stability)",
            DataType::Int64,
        );
        m.stability = Some(vgi::protocol::enums::stability::VOLATILE.to_string());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("min_val", 0, "int64", "Minimum value (inclusive)"),
            ArgSpec::column("max_val", 1, "int64", "Maximum value (inclusive)"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let lo = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let hi = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        let mut rng = Rng::new(volatile_seed());
        let out: Int64Array = (0..lo.len())
            .map(|i| {
                (!lo.is_null(i) && !hi.is_null(i)).then(|| rng.range_i64(lo.value(i), hi.value(i)))
            })
            .collect();
        result(params, arc(out))
    }
}

/// `random_bytes(seed_const, len_const)` — deterministic blobs.
pub struct RandomBytesFunction;
impl ScalarFunction for RandomBytesFunction {
    fn name(&self) -> &str {
        "random_bytes"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Generate pseudo-random binary blobs from seed and length",
            DataType::Binary,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("seed", 0, "int64", "Seed for pseudo-random byte generation"),
            ArgSpec::const_arg("byte_length", 1, "int64", "Output blob length in bytes"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let seed = params.arguments.const_i64(0).unwrap_or(0);
        let len = params.arguments.const_i64(1).unwrap_or(0);
        if len < 0 {
            return Err(RpcError::value_error("byte_length must be >= 0"));
        }
        let n = output_len(batch);
        let mut rng = Rng::new(seed as u64);
        let out: BinaryArray = (0..n)
            .map(|_| Some((0..len).map(|_| rng.byte()).collect::<Vec<u8>>()))
            .collect();
        result(params, arc(out))
    }
}

// ---------------------------------------------------------------------------
// type introspection (overloaded)
// ---------------------------------------------------------------------------

/// `type_info(v)` — return a fixed label naming the input type.
pub struct TypeInfo(&'static str, DataType);
impl ScalarFunction for TypeInfo {
    fn name(&self) -> &str {
        "type_info"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            &format!("Return type name for {} input", self.0),
            DataType::Utf8,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column_typed("v", 0, self.1.clone(), "Input value")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let c = batch.column(0);
        let out: StringArray = (0..c.len())
            .map(|i| (!c.is_null(i)).then(|| self.0.to_string()))
            .collect();
        result(params, arc(out))
    }
}

/// `pair_type(a, b)` — return a fixed label naming the two input types.
pub struct PairType(&'static str, DataType, DataType);
impl ScalarFunction for PairType {
    fn name(&self) -> &str {
        "pair_type"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            &format!("Return type pair name for {}", self.0),
            DataType::Utf8,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column_typed("a", 0, self.1.clone(), "First value"),
            ArgSpec::column_typed("b", 1, self.2.clone(), "Second value"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let a = batch.column(0);
        let b = batch.column(1);
        let out: StringArray = (0..a.len())
            .map(|i| (!a.is_null(i) && !b.is_null(i)).then(|| self.0.to_string()))
            .collect();
        result(params, arc(out))
    }
}

/// `any_mixed(a_any, b)` — `any+<type>: {b}` per row.
pub struct AnyMixed(&'static str, DataType);
impl ScalarFunction for AnyMixed {
    fn name(&self) -> &str {
        "any_mixed"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(&format!("Any+{} dispatch", self.0), DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::any_column("a", 0, "Any type value"),
            ArgSpec::column_typed("b", 1, self.1.clone(), "Second value"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let b = batch.column(1);
        let out: StringArray = (0..b.len())
            .map(|i| {
                (!b.is_null(i)).then(|| {
                    let v = if let Some(s) = b.as_string_opt::<i32>() {
                        s.value(i).to_string()
                    } else if let Some(n) =
                        vgi::numeric::array_value_i64(&arrow_array::make_array(b.to_data()), i)
                    {
                        n.to_string()
                    } else {
                        String::new()
                    };
                    format!("any+{}: {}", self.0, v)
                })
            })
            .collect();
        result(params, arc(out))
    }
}

// ---------------------------------------------------------------------------
// settings / secrets / auth
// ---------------------------------------------------------------------------

/// `multiply_by_setting(value)` — value * setting `multiplier`.
pub struct MultiplyBySettingFunction;
impl ScalarFunction for MultiplyBySettingFunction {
    fn name(&self) -> &str {
        "multiply_by_setting"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Multiply the input value by a setting value",
            DataType::Int64,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "value",
            0,
            "int64",
            "Integer value to multiply",
        )]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let mult = params.settings.get_i64("multiplier").unwrap_or(1);
        let a = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let out: Int64Array = (0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i) * mult))
            .collect();
        result(params, arc(out))
    }
}

/// `scale_by_setting(value)` — value * the float setting `scale_factor`.
///
/// Reads a `DOUBLE` session setting via `Settings::get_f64` (the float-typed
/// settings accessor), distinct from `multiply_by_setting`'s integer `get_i64`.
pub struct ScaleBySettingFunction;
impl ScalarFunction for ScaleBySettingFunction {
    fn name(&self) -> &str {
        "scale_by_setting"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret(
            "Scale the input value by the float setting `scale_factor`",
            DataType::Float64,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "float64", "Value to scale")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let scale = params.settings.get_f64("scale_factor").unwrap_or(1.0);
        let v = arrow_cast::cast(batch.column(0), &DataType::Float64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let a = v.as_primitive::<arrow_array::types::Float64Type>();
        let out: Float64Array = (0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i) * scale))
            .collect();
        result(params, arc(out))
    }
}

/// `return_secret_value()` — JSON of the resolved `vgi_example` secret.
pub struct ReturnSecretValueFunction;
impl ScalarFunction for ReturnSecretValueFunction {
    fn name(&self) -> &str {
        "return_secret_value"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Return a secret's value", DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn secret_lookups(&self, _params: &BindParams) -> Vec<SecretLookup> {
        vec![SecretLookup {
            secret_type: "vgi_example".to_string(),
            scope: None,
            name: None,
        }]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let fields = params
            .secrets
            .by_name
            .values()
            .next()
            .cloned()
            .unwrap_or_default();
        let json = serde_json::to_string(&fields).unwrap_or_else(|_| "{}".to_string());
        let n = output_len(batch);
        let out: StringArray = (0..n).map(|_| Some(json.clone())).collect();
        result(params, arc(out))
    }
}

/// `secret_field()` — exercise the `Secrets::named_field` / `Secrets::field`
/// accessors over the resolved `vgi_example` secret.
///
/// `named_field` looks up a field on a specific secret; `field` returns the
/// first secret of any name carrying that field. Both render the underlying
/// (numeric) value to a string via the secret value renderer.
pub struct SecretFieldFunction;
impl ScalarFunction for SecretFieldFunction {
    fn name(&self) -> &str {
        "secret_field"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Look up secret fields by name", DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn secret_lookups(&self, _params: &BindParams) -> Vec<SecretLookup> {
        vec![SecretLookup {
            secret_type: "vgi_example".to_string(),
            scope: None,
            name: None,
        }]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        // `port` is a named field on the first secret of type `vgi_example`.
        // Secrets are keyed by their DuckDB secret name (e.g. `test_secret`),
        // not by type, so select by type and read the field — mirroring the Go
        // worker's `namedSecretField`.
        let port = params
            .secrets
            .of_type("vgi_example")
            .find_map(|m| m.get("port").cloned())
            .unwrap_or_default();
        let name = params.secrets.field("secret_string").unwrap_or_default();
        let s = format!("port={port};name={name}");
        let n = output_len(batch);
        let out: StringArray = (0..n).map(|_| Some(s.clone())).collect();
        result(params, arc(out))
    }
}

/// `whoami(x)` — authenticated principal or "anonymous".
pub struct WhoAmIFunction;
impl ScalarFunction for WhoAmIFunction {
    fn name(&self) -> &str {
        "whoami"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta_ret("Return the authenticated principal", DataType::Utf8)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "int64", "dummy input")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let who = params
            .auth_principal
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "anonymous".to_string());
        let n = batch.num_rows();
        let out: StringArray = (0..n).map(|_| Some(who.clone())).collect();
        result(params, arc(out))
    }
}
