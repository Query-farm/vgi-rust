//! Geospatial scalar fixtures (geo_distance_*, geo_centroid_*).

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, Float64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields};
use vgi::function::{ArgSpec, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi_rpc::{Result, RpcError};

use super::util::{arc, result};

fn point_struct_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("lat", DataType::Float64, true),
        Field::new("lon", DataType::Float64, true),
    ]))
}

/// Extract parallel (lat, lon) f64 vectors from a point column of any of the
/// three supported shapes: struct{lat,lon}, list<f64>, fixed_size_list<f64,2>.
fn lat_lon(arr: &dyn Array) -> Result<(Vec<Option<f64>>, Vec<Option<f64>>)> {
    let n = arr.len();
    match arr.data_type() {
        DataType::Struct(_) => {
            let sa = arr.as_any().downcast_ref::<StructArray>().unwrap();
            let lat = sa
                .column_by_name("lat")
                .ok_or_else(|| RpcError::runtime_error("point missing lat"))?;
            let lon = sa
                .column_by_name("lon")
                .ok_or_else(|| RpcError::runtime_error("point missing lon"))?;
            Ok((to_f64(lat), to_f64(lon)))
        }
        DataType::List(_) => {
            let la = arr.as_list::<i32>();
            let mut lat = Vec::with_capacity(n);
            let mut lon = Vec::with_capacity(n);
            for i in 0..n {
                if la.is_null(i) {
                    lat.push(None);
                    lon.push(None);
                    continue;
                }
                let v = la.value(i);
                let f = to_f64(&v);
                lat.push(f.first().copied().flatten());
                lon.push(f.get(1).copied().flatten());
            }
            Ok((lat, lon))
        }
        DataType::FixedSizeList(_, _) => {
            let fa = arr.as_fixed_size_list();
            let mut lat = Vec::with_capacity(n);
            let mut lon = Vec::with_capacity(n);
            for i in 0..n {
                if fa.is_null(i) {
                    lat.push(None);
                    lon.push(None);
                    continue;
                }
                let v = fa.value(i);
                let f = to_f64(&v);
                lat.push(f.first().copied().flatten());
                lon.push(f.get(1).copied().flatten());
            }
            Ok((lat, lon))
        }
        other => Err(RpcError::runtime_error(format!("unsupported point type {other:?}"))),
    }
}

fn to_f64(arr: &dyn Array) -> Vec<Option<f64>> {
    // Cast to Float64 first — DuckDB list/struct point coords arrive as
    // decimal, int, or float depending on the literal.
    let casted = match arrow_cast::cast(arr, &DataType::Float64) {
        Ok(c) => c,
        Err(_) => return vec![None; arr.len()],
    };
    let a = casted.as_primitive::<Float64Type>();
    (0..a.len())
        .map(|i| (!a.is_null(i)).then(|| a.value(i)))
        .collect()
}

fn point_arg(name: &str, pos: i32, ty: DataType) -> ArgSpec {
    ArgSpec::column_typed(name, pos, ty, "point")
}

fn list_f64() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
}
fn fixed_f64() -> DataType {
    DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2)
}

/// `geo_distance_<shape>(p1, p2)` — euclidean distance between two points.
pub struct GeoDistance(&'static str, DataType);
impl ScalarFunction for GeoDistance {
    fn name(&self) -> &str {
        match self.0 {
            "struct" => "geo_distance_struct",
            "list" => "geo_distance_list",
            _ => "geo_distance_fixed",
        }
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: format!("Euclidean distance between two {} points", self.0),
            return_type: Some(DataType::Float64),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            point_arg("p1", 0, self.1.clone()),
            point_arg("p2", 1, self.1.clone()),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let (lat1, lon1) = lat_lon(batch.column(0).as_ref())?;
        let (lat2, lon2) = lat_lon(batch.column(1).as_ref())?;
        let out: Float64Array = (0..lat1.len())
            .map(|i| match (lat1[i], lon1[i], lat2[i], lon2[i]) {
                (Some(a), Some(b), Some(c), Some(d)) => {
                    Some(((c - a).powi(2) + (d - b).powi(2)).sqrt())
                }
                _ => None,
            })
            .collect();
        result(params, arc(out))
    }
}

/// `geo_centroid_<shape>(points...)` — average of N points → struct{lat,lon}.
pub struct GeoCentroid(&'static str, DataType);
impl ScalarFunction for GeoCentroid {
    fn name(&self) -> &str {
        match self.0 {
            "struct" => "geo_centroid_struct",
            "list" => "geo_centroid_list",
            _ => "geo_centroid_fixed",
        }
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: format!("Centroid of N {} points", self.0),
            return_type: Some(point_struct_type()),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![point_arg("points", 0, self.1.clone()).varargs()]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let n_cols = batch.num_columns();
        let n_rows = batch.num_rows();
        let mut cols: Vec<(Vec<Option<f64>>, Vec<Option<f64>>)> = Vec::with_capacity(n_cols);
        for i in 0..n_cols {
            cols.push(lat_lon(batch.column(i).as_ref())?);
        }
        let mut lat_out = Vec::with_capacity(n_rows);
        let mut lon_out = Vec::with_capacity(n_rows);
        for r in 0..n_rows {
            let mut slat = 0.0;
            let mut slon = 0.0;
            let mut ok = true;
            for (lat, lon) in &cols {
                match (lat[r], lon[r]) {
                    (Some(a), Some(b)) => {
                        slat += a;
                        slon += b;
                    }
                    _ => ok = false,
                }
            }
            if ok {
                lat_out.push(Some(slat / n_cols as f64));
                lon_out.push(Some(slon / n_cols as f64));
            } else {
                lat_out.push(None);
                lon_out.push(None);
            }
        }
        let lat = Float64Array::from(lat_out);
        let lon = Float64Array::from(lon_out);
        let sa = StructArray::from(vec![
            (
                Arc::new(Field::new("lat", DataType::Float64, true)),
                arc(lat),
            ),
            (
                Arc::new(Field::new("lon", DataType::Float64, true)),
                arc(lon),
            ),
        ]);
        result(params, arc(sa))
    }
}

/// Register all geo fixtures.
pub fn register(w: &mut vgi::Worker) {
    let pstruct = point_struct_type();
    w.register_scalar(GeoDistance("struct", pstruct.clone()));
    w.register_scalar(GeoDistance("list", list_f64()));
    w.register_scalar(GeoDistance("fixed", fixed_f64()));
    w.register_scalar(GeoCentroid("struct", pstruct));
    w.register_scalar(GeoCentroid("list", list_f64()));
    w.register_scalar(GeoCentroid("fixed", fixed_f64()));
}
