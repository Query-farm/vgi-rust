// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Fixture `COPY ... FROM` format reader for VGI integration tests.
//!
//! `ExampleLinesCopyFrom` registers the SQL format `example_lines` — a toy
//! delimited-text reader. It exercises the full COPY-FROM path plus the option
//! machinery: a defaulted option (`delimiter`), a `BIGINT` option with a range
//! constraint (`skip_rows`), a required option (`null_string`), and an
//! enum/`choices` option (`on_error`).
//!
//! Usage:
//!
//! ```sql
//! CREATE TABLE t (a INTEGER, b VARCHAR);
//! COPY t FROM '/path/data.txt' (FORMAT example_lines, null_string 'NA');
//! ```
//!
//! Mirrors the Python `vgi._test_fixtures.copy_from.ExampleLinesCopyFromFunction`.

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use vgi::copy_from::{CopyFromFunction, CopyFromReadContext};
use vgi::function::{ArgSpec, BindParams, FunctionMetadata};
use vgi::secrets::SecretLookup;
use vgi_rpc::{OutputCollector, Result, RpcError};

/// Register the COPY-FROM fixtures (delimited reader + secret-forwarding reader).
pub fn register(w: &mut vgi::Worker) {
    w.register_copy_from(ExampleLinesCopyFrom);
    w.register_copy_from(SecretLinesCopyFrom);
}

/// Toy delimited-text `COPY ... FROM` reader (test fixture).
struct ExampleLinesCopyFrom;

impl CopyFromFunction for ExampleLinesCopyFrom {
    fn format(&self) -> &str {
        "example_lines"
    }

    fn handler_name(&self) -> &str {
        "example_lines_copy_reader"
    }

    fn comment(&self) -> Option<String> {
        Some("Toy delimited-text reader for tests".to_string())
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Read a delimited text file into the COPY target table".to_string(),
            tags: vec![
                ("category".to_string(), "copy_from".to_string()),
                ("stability".to_string(), "test".to_string()),
            ],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        // COPY options arrive as named arguments (position -1). `file_path` is
        // supplied by the COPY statement, not as an option.
        vec![
            ArgSpec::column("null_string", -1, "varchar", "Token parsed as SQL NULL"),
            ArgSpec::column("delimiter", -1, "varchar", "Field separator"),
            ArgSpec::column(
                "skip_rows",
                -1,
                "int64",
                "Leading lines to skip before data",
            ),
            ArgSpec::column(
                "on_error",
                -1,
                "varchar",
                "Behavior on a row whose column count does not match the target",
            ),
        ]
    }

    fn read(
        &self,
        ctx: &CopyFromReadContext,
        _out: &mut OutputCollector,
    ) -> Result<Vec<RecordBatch>> {
        // Worker-side option enforcement (required / choices / range), mirroring
        // the Python dataclass validation. Unknown options are rejected upstream
        // by the C++ extension at bind.
        let null_string = ctx.options.named_str("null_string").ok_or_else(|| {
            RpcError::value_error("example_lines: required option 'null_string' is missing")
        })?;
        let delimiter = ctx
            .options
            .named_str("delimiter")
            .unwrap_or_else(|| ",".to_string());
        if delimiter.is_empty() {
            return Err(RpcError::value_error(
                "example_lines: 'delimiter' must not be empty",
            ));
        }
        let skip_rows = ctx.options.named_i64("skip_rows").unwrap_or(0);
        if skip_rows < 0 {
            return Err(RpcError::value_error(
                "example_lines: 'skip_rows' must be >= 0",
            ));
        }
        let skip_rows = skip_rows as usize;
        let on_error = ctx
            .options
            .named_str("on_error")
            .unwrap_or_else(|| "fail".to_string());
        if on_error != "fail" && on_error != "skip" {
            return Err(RpcError::value_error(format!(
                "example_lines: 'on_error' must be one of ['fail', 'skip'], got {on_error:?}"
            )));
        }

        let content = std::fs::read_to_string(ctx.path).map_err(|e| {
            RpcError::runtime_error(format!("example_lines: cannot read {}: {e}", ctx.path))
        })?;

        let schema = ctx.expected_schema.clone();
        let ncols = schema.fields().len();

        // Parse rows (column-count validated against the target).
        let mut rows: Vec<Vec<String>> = Vec::new();
        for (i, line) in content.lines().enumerate() {
            if i < skip_rows {
                continue;
            }
            if line.is_empty() {
                continue;
            }
            let cells: Vec<&str> = line.split(&delimiter).collect();
            if cells.len() != ncols {
                if on_error == "skip" {
                    continue;
                }
                return Err(RpcError::value_error(format!(
                    "example_lines: row has {} fields, expected {ncols}: {line:?}",
                    cells.len()
                )));
            }
            rows.push(cells.into_iter().map(|c| c.to_string()).collect());
        }

        // Build one string column per target field (NULL where the cell equals
        // null_string), then cast each to the target type. DuckDB inserts no
        // cast between the scan and the INSERT, so the output must match exactly.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(ncols);
        for c in 0..ncols {
            let vals: Vec<Option<String>> = rows
                .iter()
                .map(|r| {
                    let v = &r[c];
                    if *v == null_string {
                        None
                    } else {
                        Some(v.clone())
                    }
                })
                .collect();
            let str_arr = StringArray::from(vals);
            let casted = arrow_cast::cast(&str_arr, schema.field(c).data_type()).map_err(|e| {
                RpcError::runtime_error(format!(
                    "example_lines: cast column {} to {}: {e}",
                    schema.field(c).name(),
                    schema.field(c).data_type()
                ))
            })?;
            columns.push(casted);
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| RpcError::runtime_error(format!("example_lines: build batch: {e}")))?;
        Ok(vec![batch])
    }
}

/// `COPY ... FROM` reader that forwards a `CREATE SECRET` credential. Exercises
/// the COPY-FROM secret-bind hook (`secret_lookups`): it requests the
/// `secret_type` secret scoped to the source path during bind, and `read` emits a
/// single VARCHAR row holding the resolved secret's `api_key` (or `NONE`) — so a
/// test can assert the caller's secret reached the reader. Mirrors the Python
/// `SecretLinesCopyFromFunction`.
struct SecretLinesCopyFrom;

impl SecretLinesCopyFrom {
    fn secret_type(options: &vgi::arguments::Arguments) -> String {
        options
            .named_str("secret_type")
            .unwrap_or_else(|| "vgi_example".to_string())
    }
}

impl CopyFromFunction for SecretLinesCopyFrom {
    fn format(&self) -> &str {
        "secret_lines_in"
    }

    fn handler_name(&self) -> &str {
        "secret_lines_reader"
    }

    fn comment(&self) -> Option<String> {
        Some("Reader that forwards a CREATE SECRET credential (test fixture)".to_string())
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Emit the resolved secret's api_key as a single VARCHAR row".to_string(),
            tags: vec![
                ("category".to_string(), "copy_from".to_string()),
                ("stability".to_string(), "test".to_string()),
            ],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "secret_type",
            -1,
            "varchar",
            "Secret type to fetch, scoped by the source path",
        )]
    }

    fn secret_lookups(&self, params: &BindParams) -> Vec<SecretLookup> {
        // Request the source-scoped secret; the framework's two-phase secret bind
        // resolves it and surfaces it on ctx.params.secrets at read time.
        let Some(cf) = params.copy_from.as_ref() else {
            return Vec::new();
        };
        vec![SecretLookup {
            secret_type: Self::secret_type(&params.arguments),
            scope: Some(cf.file_path.clone()),
            name: None,
        }]
    }

    fn read(
        &self,
        ctx: &CopyFromReadContext,
        _out: &mut OutputCollector,
    ) -> Result<Vec<RecordBatch>> {
        let secret_type = Self::secret_type(ctx.options);
        let api_key = ctx
            .params
            .secrets
            .for_scope_of_type(ctx.path, &secret_type)
            .and_then(|m| m.get("api_key").cloned())
            .unwrap_or_else(|| "NONE".to_string());

        let schema = ctx.expected_schema.clone();
        if schema.fields().len() != 1 {
            return Err(RpcError::value_error(format!(
                "secret_lines_in: expected a single-column target, got {}",
                schema.fields().len()
            )));
        }
        let str_arr = StringArray::from(vec![Some(api_key)]);
        let casted: ArrayRef = arrow_cast::cast(&str_arr, schema.field(0).data_type())
            .map_err(|e| RpcError::runtime_error(format!("secret_lines_in: cast: {e}")))?;
        let batch = RecordBatch::try_new(schema, vec![casted])
            .map_err(|e| RpcError::runtime_error(format!("secret_lines_in: build batch: {e}")))?;
        Ok(vec![batch])
    }
}
