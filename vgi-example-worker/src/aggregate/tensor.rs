// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `nest_tensor(value, axes_struct)` — collect a group's rows into a dense N-D
//! tensor plus per-axis sorted coordinate lists. Output is a single `result`
//! struct column `{tensor: list^N<value>, axes: struct{axis: list<coord>}}`.
//!
//! Ports `vgi-python/_test_fixtures/nest_tensor.py` (the aggregate half).
//! Coordinate types are integers in the fixtures, so coords are compared as
//! i64; cell values keep their original Arrow type via `take`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StructArray, UInt32Array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use vgi::aggregate::{AggregateBindParams, AggregateFunction};
use vgi::function::{ArgSpec, BindResponse, FunctionMetadata};
use vgi_rpc::{Result, RpcError};

use super::agg_meta;

pub struct NestTensorFunction;

fn nest_err(msg: impl Into<String>) -> RpcError {
    RpcError::value_error(format!("NestTensorError: nest_tensor: {}", msg.into()))
}

/// Count the `List` nesting depth of a type.
fn list_depth(mut t: &DataType) -> usize {
    let mut d = 0;
    while let DataType::List(f) | DataType::LargeList(f) = t {
        d += 1;
        t = f.data_type();
    }
    d
}

/// Wrap `inner` in `depth` levels of `List<item>`.
fn nested_list_type(inner: DataType, depth: usize) -> DataType {
    let mut t = inner;
    for _ in 0..depth {
        t = DataType::List(Arc::new(Field::new("item", t, true)));
    }
    t
}

impl AggregateFunction for NestTensorFunction {
    fn name(&self) -> &str {
        "nest_tensor"
    }
    fn metadata(&self) -> FunctionMetadata {
        agg_meta("Collect rows into a dense N-D tensor plus per-axis coordinates")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("value", 0, "any", "Tensor cell value"),
            ArgSpec::column("axes", 1, "any", "Struct of axis coordinates"),
        ]
    }
    fn on_bind(&self, params: &AggregateBindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .as_ref()
            .ok_or_else(|| nest_err("expected 2 arguments (value, axes struct)"))?;
        if input.fields().len() < 2 {
            return Err(nest_err("expected 2 arguments (value, axes struct)"));
        }
        let value_type = input.field(0).data_type().clone();
        let DataType::Struct(axis_fields) = input.field(1).data_type().clone() else {
            return Err(nest_err(format!(
                "second argument must be a struct, got {}",
                input.field(1).data_type()
            )));
        };
        if axis_fields.is_empty() {
            return Err(nest_err("axes struct must have at least one field"));
        }
        for f in axis_fields.iter() {
            match f.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    return Err(nest_err(format!(
                        "axis '{}' has floating-point type {}; floats are not supported \
                         as coord types (NaN breaks equality)",
                        f.name(),
                        f.data_type()
                    )));
                }
                DataType::Struct(_)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Map(_, _) => {
                    return Err(nest_err(format!(
                        "axis '{}' has nested type {}; only scalar coord types are supported",
                        f.name(),
                        f.data_type()
                    )));
                }
                _ => {}
            }
        }
        let n = axis_fields.len();
        let tensor_type = nested_list_type(value_type, n);
        let axes_out: Fields = axis_fields
            .iter()
            .map(|f| {
                Field::new(
                    f.name(),
                    DataType::List(Arc::new(Field::new("item", f.data_type().clone(), true))),
                    true,
                )
            })
            .collect::<Vec<_>>()
            .into();
        let result_type = DataType::Struct(
            vec![
                Field::new("tensor", tensor_type, true),
                Field::new("axes", DataType::Struct(axes_out), true),
            ]
            .into(),
        );
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("result", result_type, true)])),
            opaque_data: Vec::new(),
        })
    }

    fn initial_state(&self) -> Vec<u8> {
        Vec::new()
    }

    fn update(
        &self,
        states: &mut HashMap<i64, Vec<u8>>,
        group_ids: &Int64Array,
        columns: &[ArrayRef],
    ) -> Result<()> {
        let value = &columns[0];
        let axes = &columns[1];
        let parent = Arc::new(Schema::new(vec![
            Field::new("value", value.data_type().clone(), true),
            Field::new("axes", axes.data_type().clone(), true),
        ]));
        // Group row indices by gid.
        let mut by_gid: HashMap<i64, Vec<u32>> = HashMap::new();
        for i in 0..group_ids.len() {
            by_gid.entry(group_ids.value(i)).or_default().push(i as u32);
        }
        for (gid, idx) in by_gid {
            let take_idx = UInt32Array::from(idx);
            let v = arrow_select::take::take(value, &take_idx, None).map_err(cvt)?;
            let a = arrow_select::take::take(axes, &take_idx, None).map_err(cvt)?;
            let batch = RecordBatch::try_new(parent.clone(), vec![v, a]).map_err(cvt)?;
            let merged = match states.get(&gid).filter(|s| !s.is_empty()) {
                Some(prior) => {
                    let pb = vgi::ipc::read_batch(prior)?;
                    arrow_select::concat::concat_batches(&parent, [&pb, &batch]).map_err(cvt)?
                }
                None => batch,
            };
            states.insert(gid, vgi::ipc::write_batch(&merged)?);
        }
        Ok(())
    }

    fn combine(&self, t: Vec<u8>, s: Vec<u8>) -> Result<Vec<u8>> {
        if s.is_empty() {
            return Ok(t);
        }
        if t.is_empty() {
            return Ok(s);
        }
        let tb = vgi::ipc::read_batch(&t)?;
        let sb = vgi::ipc::read_batch(&s)?;
        let merged = arrow_select::concat::concat_batches(&tb.schema(), [&tb, &sb]).map_err(cvt)?;
        vgi::ipc::write_batch(&merged)
    }

    fn finalize(
        &self,
        output_schema: &SchemaRef,
        group_ids: &Int64Array,
        states: &[Option<Vec<u8>>],
    ) -> Result<RecordBatch> {
        // Resolve the output struct sub-types from the bound schema.
        let DataType::Struct(result_fields) = output_schema.field(0).data_type() else {
            return Err(nest_err("output schema field 0 is not a struct"));
        };
        let tensor_field = result_fields[0].clone();
        let DataType::Struct(axes_out_fields) = result_fields[1].data_type().clone() else {
            return Err(nest_err("axes output is not a struct"));
        };
        let axis_names: Vec<String> = axes_out_fields.iter().map(|f| f.name().clone()).collect();
        let n_axes = axis_names.len();

        // Per-group leaf value arrays + shapes; per-axis per-group coord arrays.
        let mut leaf_arrays: Vec<ArrayRef> = Vec::new();
        let mut shapes: Vec<Vec<usize>> = Vec::new();
        let mut axis_coord_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); n_axes];
        // Element types for empty fallbacks.
        let value_elem = match tensor_field.data_type() {
            DataType::List(f) => innermost(f.data_type()),
            other => other.clone(),
        };

        for st in states.iter().take(group_ids.len()) {
            let table = st.as_ref().filter(|b| !b.is_empty());
            match table {
                None => {
                    shapes.push(vec![0; n_axes]);
                    leaf_arrays.push(arrow_array::new_empty_array(&value_elem));
                    for (a, name) in axis_names.iter().enumerate() {
                        let ct = coord_type(&axes_out_fields, name);
                        axis_coord_arrays[a].push(arrow_array::new_empty_array(&ct));
                    }
                }
                Some(bytes) => {
                    let batch = vgi::ipc::read_batch(bytes)?;
                    let value_col = batch.column(0);
                    let axes_col = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| nest_err("axes column is not a struct"))?;
                    let n_rows = batch.num_rows();
                    // Generic coord ordering via the Arrow row format: per axis,
                    // comparable row-byte keys (lexicographic == value order),
                    // so any scalar coord type (int, string, date, …) works.
                    let mut shape = Vec::with_capacity(n_axes);
                    let mut idx_maps: Vec<HashMap<Vec<u8>, usize>> = Vec::with_capacity(n_axes);
                    let mut row_keys: Vec<Vec<Vec<u8>>> = Vec::with_capacity(n_axes);
                    for a in 0..n_axes {
                        let field_arr = axes_col.column(a).clone();
                        let conv = arrow_row::RowConverter::new(vec![arrow_row::SortField::new(
                            field_arr.data_type().clone(),
                        )])
                        .map_err(cvt)?;
                        let rows = conv.convert_columns(&[field_arr.clone()]).map_err(cvt)?;
                        let keys: Vec<Vec<u8>> =
                            (0..n_rows).map(|r| rows.row(r).as_ref().to_vec()).collect();
                        let mut first_row: HashMap<Vec<u8>, u32> = HashMap::new();
                        for r in 0..n_rows {
                            if field_arr.is_valid(r) {
                                first_row.entry(keys[r].clone()).or_insert(r as u32);
                            }
                        }
                        let mut distinct: Vec<Vec<u8>> = first_row.keys().cloned().collect();
                        distinct.sort(); // byte-lexicographic == value order
                        let idx_map: HashMap<Vec<u8>, usize> = distinct
                            .iter()
                            .enumerate()
                            .map(|(i, k)| (k.clone(), i))
                            .collect();
                        shape.push(distinct.len());
                        let rep: Vec<u32> = distinct.iter().map(|k| first_row[k]).collect();
                        let coord_arr =
                            arrow_select::take::take(&field_arr, &UInt32Array::from(rep), None)
                                .map_err(cvt)?;
                        axis_coord_arrays[a].push(coord_arr);
                        idx_maps.push(idx_map);
                        row_keys.push(keys);
                    }
                    let total: usize = shape.iter().product();
                    // Map each source row to its row-major cell index. A null
                    // coord inside a non-null axes struct is an error.
                    let mut take_idx: Vec<Option<u32>> = vec![None; total];
                    for r in 0..n_rows {
                        if !axes_col.is_valid(r) {
                            continue;
                        }
                        let mut flat = 0usize;
                        for a in 0..n_axes {
                            if !axes_col.column(a).is_valid(r) {
                                return Err(nest_err(format!(
                                    "null coord value for axis '{}'",
                                    axis_names[a]
                                )));
                            }
                            let d = idx_maps[a][&row_keys[a][r]];
                            flat = flat * shape[a] + d;
                        }
                        if take_idx[flat].is_some() {
                            return Err(nest_err(format!(
                                "duplicate coordinate (axes {})",
                                axis_names.join(", ")
                            )));
                        }
                        take_idx[flat] = Some(r as u32);
                    }
                    let leaf =
                        arrow_select::take::take(value_col, &UInt32Array::from(take_idx), None)
                            .map_err(cvt)?;
                    leaf_arrays.push(leaf);
                    shapes.push(shape);
                }
            }
        }

        // Build the depth-N tensor list array (one entry per group).
        let leaf_refs: Vec<&dyn Array> = leaf_arrays.iter().map(|a| a.as_ref()).collect();
        let flat_leaf = arrow_select::concat::concat(&leaf_refs).map_err(cvt)?;
        let tensor = build_nested(flat_leaf, &shapes, n_axes)?;

        // Build axes struct: one list<coord> per axis, one list per group.
        let mut axes_cols: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(n_axes);
        for a in 0..n_axes {
            let refs: Vec<&dyn Array> = axis_coord_arrays[a].iter().map(|x| x.as_ref()).collect();
            let flat = arrow_select::concat::concat(&refs).map_err(cvt)?;
            let lens: Vec<usize> = (0..group_ids.len()).map(|g| shapes[g][a]).collect();
            let list = build_one_level(flat, &lens)?;
            axes_cols.push((axes_out_fields[a].clone(), list));
        }
        let axes_struct = StructArray::from(axes_cols);

        let result = StructArray::from(vec![
            (tensor_field.clone(), tensor),
            (
                Arc::new(Field::new(
                    "axes",
                    DataType::Struct(axes_out_fields.clone()),
                    true,
                )),
                Arc::new(axes_struct) as ArrayRef,
            ),
        ]);
        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result)]).map_err(cvt)
    }
}

fn cvt(e: impl std::fmt::Display) -> RpcError {
    RpcError::runtime_error(e.to_string())
}

/// `unnest_tensor(t)` scalar — invert `nest_tensor`: for each input struct,
/// return a `list<struct{value, axes}>` enumerating every cell of the axes
/// Cartesian product (row-major, including null-valued cells).
pub struct UnnestTensorFunction;

/// Walk `depth` `List` levels to the innermost element type.
fn unwrap_list(mut t: DataType, depth: usize) -> DataType {
    for _ in 0..depth {
        t = match t {
            DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
            other => other,
        };
    }
    t
}

fn unnest_output_type(struct_type: &DataType) -> Result<DataType> {
    let DataType::Struct(fields) = struct_type else {
        return Err(nest_err(format!(
            "argument must be a struct, got {struct_type}"
        )));
    };
    let missing = || nest_err("struct must have 'tensor' and 'axes' fields");
    let tensor_f = fields
        .iter()
        .find(|f| f.name() == "tensor")
        .ok_or_else(missing)?;
    let axes_f = fields
        .iter()
        .find(|f| f.name() == "axes")
        .ok_or_else(missing)?;
    let DataType::Struct(axis_fields) = axes_f.data_type() else {
        return Err(nest_err("'axes' field must be a struct"));
    };
    let depth = axis_fields.len();
    let actual = list_depth(tensor_f.data_type());
    if actual != depth {
        return Err(nest_err(format!(
            "tensor nesting depth {actual} does not match number of axes {depth}"
        )));
    }
    let cell_type = unwrap_list(tensor_f.data_type().clone(), depth);
    let out_axes: Fields = axis_fields
        .iter()
        .map(|f| {
            let coord = match f.data_type() {
                DataType::List(inner) | DataType::LargeList(inner) => inner.data_type().clone(),
                other => other.clone(),
            };
            Field::new(f.name(), coord, true)
        })
        .collect::<Vec<_>>()
        .into();
    let row_type = DataType::Struct(
        vec![
            Field::new("value", cell_type, true),
            Field::new("axes", DataType::Struct(out_axes), true),
        ]
        .into(),
    );
    Ok(DataType::List(Arc::new(Field::new("item", row_type, true))))
}

/// `unnest_tensor_rows(table)` table-in-out — invert `nest_tensor`, emitting one
/// flat `{value, axes}` row per cell (LATERAL-friendly).
pub struct UnnestTensorRowsFunction;
impl vgi::table_in_out::TableInOutFunction for UnnestTensorRowsFunction {
    fn name(&self) -> &str {
        "unnest_tensor_rows"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Invert nest_tensor, streaming one row per cell (LATERAL-friendly)"
                .to_string(),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "data",
            0,
            "table",
            "Input table: one column of nest_tensor structs",
        )]
    }
    fn on_bind(&self, params: &vgi::function::BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .as_ref()
            .ok_or_else(|| nest_err("unnest_tensor_rows requires an input schema"))?;
        if input.fields().len() != 1 {
            return Err(nest_err(
                "input table must have exactly one column (the nest_tensor struct)",
            ));
        }
        let list = unnest_output_type(input.field(0).data_type())?;
        let DataType::List(rf) = list else {
            return Err(nest_err("internal: output not a list"));
        };
        let DataType::Struct(fields) = rf.data_type().clone() else {
            return Err(nest_err("internal: element not a struct"));
        };
        let out_fields: Vec<Field> = fields.iter().map(|f| f.as_ref().clone()).collect();
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(out_fields)),
            opaque_data: Vec::new(),
        })
    }
    fn process(
        &self,
        params: &vgi::function::ProcessParams,
        batch: &RecordBatch,
    ) -> Result<Vec<RecordBatch>> {
        let out_schema = &params.output_schema;
        let struct_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| nest_err("input column must be a struct"))?;
        let tensor_list = struct_arr.column_by_name("tensor").unwrap();
        let axes = struct_arr
            .column_by_name("axes")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let n_axes = axes.num_columns();
        let mut leaf: ArrayRef = tensor_list.clone();
        for _ in 0..n_axes {
            leaf = leaf
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .clone();
        }
        let axis_lists: Vec<&ListArray> = (0..n_axes)
            .map(|a| axes.column(a).as_any().downcast_ref::<ListArray>().unwrap())
            .collect();
        let axis_values: Vec<ArrayRef> = axis_lists.iter().map(|l| l.values().clone()).collect();

        let mut value_take: Vec<Option<u32>> = Vec::new();
        let mut axis_take: Vec<Vec<u32>> = vec![Vec::new(); n_axes];
        let mut leaf_start = 0u32;
        for i in 0..batch.num_rows() {
            if struct_arr.is_null(i) {
                continue;
            }
            let shape: Vec<usize> = (0..n_axes)
                .map(|a| axis_lists[a].value_length(i) as usize)
                .collect();
            let total: usize = shape.iter().product();
            let axis_off: Vec<i32> = (0..n_axes)
                .map(|a| axis_lists[a].value_offsets()[i])
                .collect();
            for k in 0..total {
                value_take.push(Some(leaf_start + k as u32));
                let mut rem = k;
                let mut dims = vec![0usize; n_axes];
                for a in (0..n_axes).rev() {
                    dims[a] = rem % shape[a];
                    rem /= shape[a];
                }
                for a in 0..n_axes {
                    axis_take[a].push(axis_off[a] as u32 + dims[a] as u32);
                }
            }
            leaf_start += total as u32;
        }
        let value_arr =
            arrow_select::take::take(&leaf, &UInt32Array::from(value_take), None).map_err(cvt)?;
        let DataType::Struct(out_axis_fields) = out_schema.field(1).data_type().clone() else {
            return Err(nest_err("axes output is not a struct"));
        };
        let mut axis_cols: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(n_axes);
        for a in 0..n_axes {
            let arr = arrow_select::take::take(
                &axis_values[a],
                &UInt32Array::from(axis_take[a].clone()),
                None,
            )
            .map_err(cvt)?;
            axis_cols.push((out_axis_fields[a].clone(), arr));
        }
        let axes_struct = StructArray::from(axis_cols);
        let out = RecordBatch::try_new(out_schema.clone(), vec![value_arr, Arc::new(axes_struct)])
            .map_err(cvt)?;
        Ok(vec![out])
    }
}

impl vgi::function::ScalarFunction for UnnestTensorFunction {
    fn name(&self) -> &str {
        "unnest_tensor"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Invert nest_tensor: list of {value, axes} structs per cell".to_string(),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "t",
            0,
            "any",
            "Struct produced by nest_tensor",
        )]
    }
    fn on_bind(&self, params: &vgi::function::BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .as_ref()
            .ok_or_else(|| nest_err("unnest_tensor requires an input schema"))?;
        let out = unnest_output_type(input.field(0).data_type())?;
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("result", out, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn process(
        &self,
        params: &vgi::function::ProcessParams,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let struct_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| nest_err("input must be a struct array"))?;
        let tensor_list = struct_arr.column_by_name("tensor").unwrap();
        let axes = struct_arr
            .column_by_name("axes")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| nest_err("'axes' must be a struct"))?;
        let n_axes = axes.num_columns();

        // Innermost leaf values (walk N list levels).
        let mut leaf: ArrayRef = tensor_list.clone();
        for _ in 0..n_axes {
            leaf = leaf
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| nest_err("tensor nesting shallower than axis count"))?
                .values()
                .clone();
        }
        // Per-axis coord lists + their flat value arrays.
        let axis_lists: Vec<&ListArray> = (0..n_axes)
            .map(|a| axes.column(a).as_any().downcast_ref::<ListArray>().unwrap())
            .collect();
        let axis_values: Vec<ArrayRef> = axis_lists.iter().map(|l| l.values().clone()).collect();

        let n_rows = batch.num_rows();
        let mut value_take: Vec<Option<u32>> = Vec::new();
        let mut axis_take: Vec<Vec<u32>> = vec![Vec::new(); n_axes];
        let mut out_offsets: Vec<i32> = vec![0];
        let mut valid: Vec<bool> = Vec::with_capacity(n_rows);
        let mut leaf_start = 0u32;
        let mut acc = 0i32;

        for i in 0..n_rows {
            if struct_arr.is_null(i) {
                valid.push(false);
                out_offsets.push(acc);
                continue;
            }
            valid.push(true);
            let shape: Vec<usize> = (0..n_axes)
                .map(|a| axis_lists[a].value_length(i) as usize)
                .collect();
            let total: usize = shape.iter().product();
            let axis_off: Vec<i32> = (0..n_axes)
                .map(|a| axis_lists[a].value_offsets()[i])
                .collect();
            for k in 0..total {
                value_take.push(Some(leaf_start + k as u32));
                // Decode row-major k into per-axis indices.
                let mut rem = k;
                let mut dims = vec![0usize; n_axes];
                for a in (0..n_axes).rev() {
                    dims[a] = rem % shape[a];
                    rem /= shape[a];
                }
                for a in 0..n_axes {
                    axis_take[a].push(axis_off[a] as u32 + dims[a] as u32);
                }
            }
            leaf_start += total as u32;
            acc += total as i32;
            out_offsets.push(acc);
        }

        let value_arr =
            arrow_select::take::take(&leaf, &UInt32Array::from(value_take), None).map_err(cvt)?;
        let DataType::List(row_field) = params.output_schema.field(0).data_type().clone() else {
            return Err(nest_err("output is not a list"));
        };
        let DataType::Struct(row_fields) = row_field.data_type().clone() else {
            return Err(nest_err("list element is not a struct"));
        };
        let DataType::Struct(out_axis_fields) = row_fields[1].data_type().clone() else {
            return Err(nest_err("axes output is not a struct"));
        };
        let mut axis_cols: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(n_axes);
        for a in 0..n_axes {
            let arr = arrow_select::take::take(
                &axis_values[a],
                &UInt32Array::from(axis_take[a].clone()),
                None,
            )
            .map_err(cvt)?;
            axis_cols.push((out_axis_fields[a].clone(), arr));
        }
        let axes_struct = StructArray::from(axis_cols);
        let row_struct = StructArray::from(vec![
            (row_fields[0].clone(), value_arr),
            (row_fields[1].clone(), Arc::new(axes_struct) as ArrayRef),
        ]);
        let nulls = arrow_buffer::NullBuffer::from(valid);
        let list = ListArray::new(
            row_field,
            OffsetBuffer::new(out_offsets.into()),
            Arc::new(row_struct),
            Some(nulls),
        );
        RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(list)]).map_err(cvt)
    }
}

fn innermost(t: &DataType) -> DataType {
    match t {
        DataType::List(f) => innermost(f.data_type()),
        other => other.clone(),
    }
}

fn coord_type(axes_out_fields: &Fields, name: &str) -> DataType {
    axes_out_fields
        .iter()
        .find(|f| f.name() == name)
        .and_then(|f| match f.data_type() {
            DataType::List(inner) => Some(inner.data_type().clone()),
            _ => None,
        })
        .unwrap_or(DataType::Int64)
}

/// Build a single `List` level partitioning `values` by per-group `lens`.
fn build_one_level(values: ArrayRef, lens: &[usize]) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(lens.len() + 1);
    let mut acc = 0i32;
    offsets.push(0i32);
    for &l in lens {
        acc += l as i32;
        offsets.push(acc);
    }
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    Ok(Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    )))
}

/// Build the depth-`n` nested list array from the flat row-major leaf values and
/// per-group shapes. One output entry per group.
fn build_nested(flat_leaf: ArrayRef, shapes: &[Vec<usize>], n: usize) -> Result<ArrayRef> {
    let mut current = flat_leaf;
    for d in (0..n).rev() {
        let mut offsets = vec![0i32];
        let mut acc = 0i32;
        for shape in shapes {
            let num_lists: usize = shape[0..d].iter().product();
            let len = shape[d];
            for _ in 0..num_lists {
                acc += len as i32;
                offsets.push(acc);
            }
        }
        let field = Arc::new(Field::new("item", current.data_type().clone(), true));
        current = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            current,
            None,
        ));
    }
    Ok(current)
}
