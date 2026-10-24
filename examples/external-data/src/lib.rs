// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! A local external-data worker with conservative partition pruning.
//!
//! The manifest supplies each partition's latitude; JSONL files supply the
//! remaining columns. A missing column during pruning is unknown, not NULL.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::Deserialize;
use vgi::diagnostics::{source_error, source_io_error, ExecutionDiagnostics, ScanStats};
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FilterHandling, FunctionMetadata, ProcessParams,
};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::vgi_rpc::{errors::Code, OutputCollector};
use vgi::{Result, RpcError};

const BATCH_SIZE: usize = 2;
const FUNCTION: &str = "read_observations";

/// The complete bound schema, including columns used only by a WHERE clause.
pub fn observations_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("latitude", DataType::Float64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("reading", DataType::Float64, true),
    ]))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Partition {
    latitude: f64,
    file: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    id: i64,
    count: u64,
    label: Option<String>,
    reading: Option<f64>,
}

/// Read a directory containing `manifest.json` and its JSONL partitions.
pub struct ReadObservations;

impl TableFunction for ReadObservations {
    fn name(&self) -> &str {
        FUNCTION
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Read partitioned local observations with exact Arrow types".into(),
            projection_pushdown: true,
            ..Default::default()
        }
        .with_filter_handling(FilterHandling::Sdk)
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "directory",
            0,
            "varchar",
            "Directory containing manifest.json and the listed JSONL files",
        )]
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        directory(&params.arguments)?;
        Ok(BindResponse {
            output_schema: observations_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let diagnostics = ExecutionDiagnostics::new(FUNCTION, &params.execution_id);
        let directory = directory(&params.arguments)?;
        let bytes = std::fs::read(directory.join("manifest.json")).map_err(|error| {
            diagnostics.annotate_error(source_io_error("read", "partition manifest", &error))
        })?;
        let partitions: Vec<Partition> = serde_json::from_slice(&bytes).map_err(|_| {
            diagnostics.annotate_error(source_error(Code::DataLoss, "decode", "partition manifest"))
        })?;
        for partition in &partitions {
            // The manifest's filename is a leaf relative to this directory.
            // Its latitude is the actual coordinate, not an approximate bound.
            let mut components = Path::new(&partition.file).components();
            if !matches!(components.next(), Some(Component::Normal(_)))
                || components.next().is_some()
                || !partition.latitude.is_finite()
            {
                return Err(diagnostics.annotate_error(source_error(
                    Code::DataLoss,
                    "validate",
                    "partition manifest",
                )));
            }
        }

        // One candidate per partition. All other columns are absent because
        // they have not been read yet; supplying NULLs here would be incorrect.
        let known = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "latitude",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(
                partitions.iter().map(|p| p.latitude).collect::<Vec<_>>(),
            ))],
        )
        .map_err(arrow_error)?;
        let keep = params
            .current_pushdown_filters
            .as_ref()
            .map(|filters| filters.may_match(&known))
            .transpose()?;
        let total_partitions = partitions.len();
        let partitions: Vec<_> = partitions
            .into_iter()
            .enumerate()
            .filter(|(index, _)| keep.as_ref().is_none_or(|mask| mask.value(*index)))
            .map(|(_, partition)| partition)
            .collect();
        let stats = ScanStats {
            chunks_skipped: (total_partitions - partitions.len()) as u64,
            bytes_read: bytes.len() as u64,
            ..Default::default()
        };

        Ok(Box::new(ObservationScan {
            directory,
            partitions: partitions.into_iter(),
            current: None,
            latitude: 0.0,
            diagnostics,
            stats,
            started: false,
            finished: false,
        }))
    }
}

fn directory(arguments: &vgi::arguments::Arguments) -> Result<PathBuf> {
    arguments
        .const_str(0)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            RpcError::value_error("directory must be a non-empty string")
                .with_code(Code::InvalidArgument)
        })
}

struct ObservationScan {
    directory: PathBuf,
    partitions: std::vec::IntoIter<Partition>,
    current: Option<BufReader<File>>,
    latitude: f64,
    diagnostics: ExecutionDiagnostics,
    stats: ScanStats,
    started: bool,
    finished: bool,
}

impl TableProducer for ObservationScan {
    fn next_batch(&mut self, out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        if !self.started {
            self.diagnostics.scan_started(out);
            self.started = true;
        }

        let mut latitudes = Vec::new();
        let mut rows = Vec::new();
        while rows.len() < BATCH_SIZE {
            if self.current.is_none() {
                let Some(partition) = self.partitions.next() else {
                    break;
                };
                let file = File::open(self.directory.join(partition.file)).map_err(|error| {
                    self.diagnostics.annotate_error(source_io_error(
                        "open",
                        "observation partition",
                        &error,
                    ))
                })?;
                self.current = Some(BufReader::new(file));
                self.latitude = partition.latitude;
                self.stats.chunks_read += 1;
            }
            let mut line = String::new();
            let bytes = self
                .current
                .as_mut()
                .unwrap()
                .read_line(&mut line)
                .map_err(|error| {
                    self.diagnostics.annotate_error(source_io_error(
                        "read",
                        "observation partition",
                        &error,
                    ))
                })?;
            if bytes == 0 {
                self.current = None;
                continue;
            }
            self.stats.bytes_read += bytes as u64;
            let row: Observation = serde_json::from_str(&line).map_err(|_| {
                self.diagnostics.annotate_error(source_error(
                    Code::DataLoss,
                    "decode",
                    "observation record",
                ))
            })?;
            self.stats.rows_read += 1;
            rows.push(row);
            latitudes.push(self.latitude);
        }

        if rows.is_empty() {
            self.diagnostics.scan_finished(out, &self.stats);
            self.finished = true;
            return Ok(None);
        }

        // Integers go directly from serde's i64/u64 into matching Arrow arrays.
        // Never route them through f64: the fixture includes values above 2^53.
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(latitudes)),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.id).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.count).collect::<Vec<_>>(),
            )),
            Arc::new(
                rows.iter()
                    .map(|r| r.label.as_deref())
                    .collect::<StringArray>(),
            ),
            Arc::new(rows.iter().map(|r| r.reading).collect::<Float64Array>()),
        ];
        // Return the FULL bound schema, even when the request projects a subset.
        // The SDK evaluates filters first, then projects and reorders columns.
        RecordBatch::try_new(observations_schema(), columns)
            .map(Some)
            .map_err(arrow_error)
    }

    fn resume_supported(&self) -> bool {
        // A live BufReader<File> is kept between ticks. main.rs serves stdio
        // only; adding HTTP would require a serializable file/offset cursor.
        false
    }
}

fn arrow_error(error: arrow_schema::ArrowError) -> RpcError {
    RpcError::runtime_error(error.to_string()).with_code(Code::Internal)
}
