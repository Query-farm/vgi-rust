// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Fixture `COPY ... TO` format writers for VGI integration tests.
//!
//! `ExampleLinesCopyTo` registers the SQL format `example_lines_out` — a toy
//! delimited-text writer, the symmetric counterpart of the `example_lines`
//! reader. `ExampleLinesOrderedCopyTo` registers `example_lines_ordered_out`,
//! the source-order variant (`ordered = true` → single-thread sink).
//!
//! Both exercise the COPY-TO Sink+Combine path plus the option machinery: a
//! required option (`null_string`), a defaulted option (`delimiter`), a BOOLEAN
//! option (`header`), and an enum/`choices` option (`on_exists`).
//!
//! Shards are buffered in `ctx.storage` (`execution_id`-scoped) by `write()` and
//! concatenated to the destination by `close()` — the cross-process-safe pattern,
//! so it works under pool rotation / HTTP.
//!
//! Usage:
//!
//! ```sql
//! COPY (SELECT * FROM t) TO '/path/out.txt'
//!   (FORMAT 'acme.example_lines_out', null_string 'NA');
//! ```
//!
//! Mirrors the Python `vgi._test_fixtures.copy_to.ExampleLinesCopyToFunction`
//! / `ExampleLinesOrderedCopyToFunction`.

use std::io::Write;

use arrow_array::{Array, RecordBatch, StringArray};
use vgi::copy_to::{CopyToCloseContext, CopyToFunction, CopyToWriteContext};
use vgi::function::{ArgSpec, BindParams, FunctionMetadata};
use vgi::ipc;
use vgi::secrets::SecretLookup;
use vgi_rpc::{Result, RpcError};

/// Append-only shard namespace (execution-scoped). Each `write()` appends one
/// IPC-serialized input batch; `close()` scans them back in append order.
const SHARD_NS: &[u8] = b"copy_to_shard";

/// Append-only namespace the secret writer counts shards under.
const SECRET_SHARD_NS: &[u8] = b"copy_to_secret_shard";

/// Register the COPY-TO fixtures (default + ordered + secret-forwarding).
pub fn register(w: &mut vgi::Worker) {
    w.register_copy_to(ExampleLinesCopyTo {
        format: "example_lines_out",
        handler: "example_lines_writer",
        comment: "Toy delimited-text writer for tests",
        description: "Write the COPY source to a delimited text file",
        ordered: false,
    });
    w.register_copy_to(ExampleLinesCopyTo {
        format: "example_lines_ordered_out",
        handler: "example_lines_ordered_writer",
        comment: "Toy delimited-text writer (ordered, single-thread sink)",
        description: "Write the COPY source to a delimited file, preserving source order",
        ordered: true,
    });
    w.register_copy_to(SecretLinesCopyTo);
}

/// Toy delimited-text `COPY ... TO` writer (test fixture). The ordered variant
/// differs only in `ordered` (and its identifiers/docs).
struct ExampleLinesCopyTo {
    format: &'static str,
    handler: &'static str,
    comment: &'static str,
    description: &'static str,
    ordered: bool,
}

/// Resolved + validated COPY options.
struct Options {
    null_string: String,
    delimiter: String,
    header: bool,
    header_repeat: i64,
    fail_on_value: String,
}

impl ExampleLinesCopyTo {
    /// Worker-side option enforcement (required / choices), mirroring the Python
    /// dataclass validation. Unknown options are rejected upstream by the C++
    /// extension at bind.
    fn parse_options(&self, options: &vgi::arguments::Arguments) -> Result<Options> {
        let null_string = options.named_str("null_string").ok_or_else(|| {
            RpcError::value_error(format!(
                "{}: required option 'null_string' is missing",
                self.format
            ))
        })?;
        let delimiter = options
            .named_str("delimiter")
            .unwrap_or_else(|| ",".to_string());
        if delimiter.is_empty() {
            return Err(RpcError::value_error(format!(
                "{}: 'delimiter' must not be empty",
                self.format
            )));
        }
        let header = options.named_bool("header").unwrap_or(false);
        let header_repeat = options.named_i64("header_repeat").unwrap_or(1);
        if !(0..=3).contains(&header_repeat) {
            return Err(RpcError::value_error(format!(
                "{}: 'header_repeat' must be between 0 and 3, got {header_repeat}",
                self.format
            )));
        }
        let on_exists = options
            .named_str("on_exists")
            .unwrap_or_else(|| "overwrite".to_string());
        if on_exists != "overwrite" && on_exists != "error" {
            return Err(RpcError::value_error(format!(
                "{}: 'on_exists' must be one of ['overwrite', 'error'], got {on_exists:?}",
                self.format
            )));
        }
        let fail_on_value = options.named_str("fail_on_value").unwrap_or_default();
        Ok(Options {
            null_string,
            delimiter,
            header,
            header_repeat,
            fail_on_value,
        })
    }
}

impl CopyToFunction for ExampleLinesCopyTo {
    fn format(&self) -> &str {
        self.format
    }

    fn handler_name(&self) -> &str {
        self.handler
    }

    fn comment(&self) -> Option<String> {
        Some(self.comment.to_string())
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: self.description.to_string(),
            tags: vec![
                ("category".to_string(), "copy_to".to_string()),
                ("stability".to_string(), "test".to_string()),
            ],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        // COPY options arrive as named arguments (position -1). `file_path` is
        // supplied by the COPY statement, not as an option.
        vec![
            ArgSpec::column("null_string", -1, "varchar", "Token written for SQL NULL"),
            ArgSpec::column("delimiter", -1, "varchar", "Field separator"),
            ArgSpec::column(
                "header",
                -1,
                "boolean",
                "Write a header row of column names",
            ),
            ArgSpec::column(
                "header_repeat",
                -1,
                "int64",
                "When header=true, write the header line this many times",
            ),
            ArgSpec::column(
                "on_exists",
                -1,
                "varchar",
                "Behavior when the destination file already exists",
            ),
            ArgSpec::column(
                "fail_on_value",
                -1,
                "varchar",
                "If non-empty, fail mid-write when a cell equals this value",
            ),
        ]
    }

    fn ordered(&self) -> bool {
        self.ordered
    }

    fn write(&self, ctx: &CopyToWriteContext, batch: &RecordBatch) -> Result<()> {
        // Validate options eagerly (surfaces e.g. a missing required option even
        // for a single-batch COPY before the terminal write).
        let opts = self.parse_options(ctx.options)?;
        // Mid-sink failure trigger: raise during a process() call when a cell's
        // string form matches fail_on_value. Exercises the in-flight
        // teardown/recovery path under a parallel sink.
        if !opts.fail_on_value.is_empty() {
            for col in batch.columns() {
                let casted = arrow_cast::cast(col, &arrow_schema::DataType::Utf8)
                    .map_err(|e| RpcError::runtime_error(format!("{}: cast: {e}", self.format)))?;
                let str_col = StringArray::from(casted.to_data());
                for r in 0..str_col.len() {
                    if !str_col.is_null(r) && str_col.value(r) == opts.fail_on_value {
                        return Err(RpcError::value_error(format!(
                            "{}: fail_on_value hit: {:?}",
                            self.format, opts.fail_on_value
                        )));
                    }
                }
            }
        }
        // Buffer one input batch as an IPC blob in execution-scoped storage.
        // `append` is atomic + race-safe across parallel sink threads/workers.
        let blob = ipc::write_batch(batch)?;
        ctx.storage.append(ctx.execution_id, SHARD_NS, b"", blob);
        Ok(())
    }

    fn close(&self, ctx: &CopyToCloseContext) -> Result<i64> {
        let opts = self.parse_options(ctx.options)?;

        // on_exists='error' refuses an existing destination.
        let on_exists = ctx
            .options
            .named_str("on_exists")
            .unwrap_or_else(|| "overwrite".to_string());
        if on_exists == "error" && std::path::Path::new(ctx.path).exists() {
            return Err(RpcError::runtime_error(format!(
                "{}: destination already exists: {}",
                self.format, ctx.path
            )));
        }

        // Read shards in append order (after_id=-1 → all; large limit drains).
        let shards = ctx
            .storage
            .scan(ctx.execution_id, SHARD_NS, b"", -1, usize::MAX);

        let mut file = std::fs::File::create(ctx.path).map_err(|e| {
            RpcError::runtime_error(format!("{}: cannot create {}: {e}", self.format, ctx.path))
        })?;

        let mut rows_written: i64 = 0;
        let mut wrote_header = false;
        for (_id, blob) in &shards {
            let batch = ipc::read_batch(blob)?;
            let names: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            if opts.header && !wrote_header {
                // header=true writes the column-name line `header_repeat` times.
                let line = names.join(&opts.delimiter);
                for _ in 0..opts.header_repeat {
                    writeln!(file, "{line}").map_err(write_err)?;
                }
                wrote_header = true;
            }
            // Cast each column to Utf8 so cell rendering matches the reader's
            // round-trip expectations (int → "1", etc.); NULL → null_string.
            let str_cols: Vec<StringArray> = batch
                .columns()
                .iter()
                .map(|col| {
                    let casted =
                        arrow_cast::cast(col, &arrow_schema::DataType::Utf8).map_err(|e| {
                            RpcError::runtime_error(format!("{}: cast: {e}", self.format))
                        })?;
                    Ok(StringArray::from(casted.to_data()))
                })
                .collect::<Result<_>>()?;
            for r in 0..batch.num_rows() {
                let cells: Vec<String> = str_cols
                    .iter()
                    .map(|c| {
                        if c.is_null(r) {
                            opts.null_string.clone()
                        } else {
                            c.value(r).to_string()
                        }
                    })
                    .collect();
                writeln!(file, "{}", cells.join(&opts.delimiter)).map_err(write_err)?;
                rows_written += 1;
            }
        }

        // Empty COPY with header=true still emits the header row(s). The source
        // column names ride the bind's input_schema.
        if opts.header && !wrote_header {
            if let Some(in_schema) = ctx.input_schema {
                let names: Vec<String> = in_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                let line = names.join(&opts.delimiter);
                for _ in 0..opts.header_repeat {
                    writeln!(file, "{line}").map_err(write_err)?;
                }
            }
        }

        file.flush().map_err(write_err)?;
        Ok(rows_written)
    }
}

fn write_err(e: std::io::Error) -> RpcError {
    RpcError::runtime_error(format!("example_lines_out: write failed: {e}"))
}

/// `COPY ... TO` writer that forwards a `CREATE SECRET` credential. Exercises the
/// COPY-TO secret-bind hook (`secret_lookups`): it requests the `secret_type`
/// secret scoped to the destination path during bind, and `close()` writes the
/// resolved secret's `api_key` (or `NONE`) plus the row count — so a test can
/// assert the caller's secret reached the writer for a secret-backed cloud write.
/// Mirrors the Python `SecretLinesCopyToFunction`.
struct SecretLinesCopyTo;

impl SecretLinesCopyTo {
    fn secret_type(options: &vgi::arguments::Arguments) -> String {
        options
            .named_str("secret_type")
            .unwrap_or_else(|| "vgi_example".to_string())
    }
}

impl CopyToFunction for SecretLinesCopyTo {
    fn format(&self) -> &str {
        "secret_lines_out"
    }

    fn handler_name(&self) -> &str {
        "secret_lines_writer"
    }

    fn comment(&self) -> Option<String> {
        Some("Writer that forwards a CREATE SECRET credential (test fixture)".to_string())
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Write the resolved secret's api_key + row count to the destination"
                .to_string(),
            tags: vec![
                ("category".to_string(), "copy_to".to_string()),
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
            "Secret type to fetch, scoped by the destination path",
        )]
    }

    fn secret_lookups(&self, params: &BindParams) -> Vec<SecretLookup> {
        // Request the destination-scoped secret; the framework's two-phase secret
        // bind resolves it and surfaces it on ctx.params.secrets at close time.
        let Some(ct) = params.copy_to.as_ref() else {
            return Vec::new();
        };
        vec![SecretLookup {
            secret_type: Self::secret_type(&params.arguments),
            scope: Some(ct.file_path.clone()),
            name: None,
        }]
    }

    fn write(&self, ctx: &CopyToWriteContext, batch: &RecordBatch) -> Result<()> {
        // Record this shard's row count (cross-process-safe append).
        ctx.storage.append(
            ctx.execution_id,
            SECRET_SHARD_NS,
            b"",
            batch.num_rows().to_string().into_bytes(),
        );
        Ok(())
    }

    fn close(&self, ctx: &CopyToCloseContext) -> Result<i64> {
        let secret_type = Self::secret_type(ctx.options);
        let api_key = ctx
            .params
            .secrets
            .for_scope_of_type(ctx.path, &secret_type)
            .and_then(|m| m.get("api_key").cloned())
            .unwrap_or_else(|| "NONE".to_string());

        let shards = ctx
            .storage
            .scan(ctx.execution_id, SECRET_SHARD_NS, b"", -1, usize::MAX);
        let mut total: i64 = 0;
        for (_id, blob) in &shards {
            if let Ok(s) = std::str::from_utf8(blob) {
                total += s.trim().parse::<i64>().unwrap_or(0);
            }
        }

        let mut file = std::fs::File::create(ctx.path).map_err(|e| {
            RpcError::runtime_error(format!("secret_lines_out: cannot create {}: {e}", ctx.path))
        })?;
        write!(file, "api_key={api_key}\nrows={total}\n").map_err(|e| {
            RpcError::runtime_error(format!("secret_lines_out: write failed: {e}"))
        })?;
        file.flush().map_err(|e| {
            RpcError::runtime_error(format!("secret_lines_out: write failed: {e}"))
        })?;
        Ok(total)
    }
}
