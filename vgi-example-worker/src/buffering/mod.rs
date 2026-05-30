//! Table-buffering example fixtures.

use std::sync::Arc;

use arrow_array::RecordBatch;
use vgi::buffering::{BufferingParams, TableBufferingFunction};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata};
use vgi::ipc;
use vgi::table_function::TableProducer;
use vgi_rpc::{Result, RpcError};

/// Register buffering fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_buffering(BufferInputFunction { name: "buffer_input", pushdown: false });
    w.register_buffering(BufferInputFunction { name: "echo_buffering", pushdown: true });
}

fn table_arg() -> ArgSpec {
    ArgSpec::column("data", 0, "table", "Input table")
}

const NS: &[u8] = b"buf";

/// Drains the per-execution buffered batch log, one batch per tick.
struct LogDrain {
    storage: Arc<vgi::buffering::BufferingStore>,
    execution_id: Vec<u8>,
    after_id: i64,
    output_schema: arrow_schema::SchemaRef,
}
impl TableProducer for LogDrain {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let rows = self
            .storage
            .scan(&self.execution_id, NS, b"", self.after_id, 1);
        let Some((id, value)) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.after_id = id;
        let batch = ipc::read_batch(&value)?;
        // Narrow to the (possibly projected) output schema by name.
        let batch = vgi::table_in_out::project_batch(&batch, &self.output_schema)?;
        Ok(Some(batch))
    }
}

/// `buffer_input` / `echo_buffering` — buffer all input, emit on finalize.
pub struct BufferInputFunction {
    name: &'static str,
    pushdown: bool,
}

impl TableBufferingFunction for BufferInputFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Collects all input batches and emits during finalization".to_string(),
            categories: vec!["utility".into(), "buffer".into()],
            projection_pushdown: self.pushdown,
            filter_pushdown: self.pushdown,
            auto_apply_filters: self.pushdown,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("buffering requires input schema"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        let bytes = ipc::write_batch(batch)?;
        params
            .storage
            .append(&params.execution_id, NS, b"", bytes);
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        // Every state_id is the execution_id; collapse to one finalize stream.
        Ok(vec![params.execution_id.clone()])
    }
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(LogDrain {
            storage: params.storage.clone(),
            execution_id: params.execution_id.clone(),
            after_id: -1,
            output_schema: params.output_schema.clone(),
        }))
    }
}
