// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Scalar example fixtures.

mod fmt;
mod geo;
mod util;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow_schema::DataType;
use util::*;
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction, ADDABLE,
    MULTIPLIABLE,
};
use vgi::numeric::{add_two, common_type_for_addition, double_first, promote_for_addition};
use vgi::secrets::SecretLookup;
use vgi_rpc::{Result, RpcError};

/// Register all scalar fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_scalar(DoubleFunction);
    w.register_scalar(AddValuesFunction);
    w.register_scalar(MultiplyFunction);
    w.register_scalar(SumValuesFunction);
    w.register_scalar(ConcatValuesIntFunction);
    w.register_scalar(ConcatValuesStrFunction);
    w.register_scalar(UpperCaseFunction);
    w.register_scalar(NullHandlingFunction);
    w.register_scalar(ConditionalMessageFunction);
    w.register_scalar(HashSeedFunction);
    w.register_scalar(BernoulliFunction);
    w.register_scalar(RandomIntFunction);
    w.register_scalar(RandomBytesFunction);
    w.register_scalar(BinaryPacketFunction);
    w.register_scalar(MultiplyBySettingFunction);
    w.register_scalar(ReturnSecretValueFunction);
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
        meta("Doubles numeric values")
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
