// Copyright 2025, 2026 Query Farm LLC - https://query.farm
use crate::protocol::dtos::{BindRequest, BindResponse};
use crate::{ipc, wire};
use arrow_schema::{Schema, SchemaRef};
use base64::Engine;
use std::sync::Arc;
use vgi_protocol::generated::protocol_types::SqlExpression;
use vgi_rpc::{Bytes, LargeBytes, Result, RpcError, VgiArrow};
pub const PROFILE: &str = "vgi.expression.duckdb.scalar.v1";
#[derive(Clone, Debug, VgiArrow)]
pub struct ProjectionRequest {
    pub bind_call: Bytes,
    pub bind_result: Bytes,
    pub expressions: Vec<LargeBytes>,
    pub result_schema: Bytes,
}
#[derive(Clone, Debug, VgiArrow)]
pub struct ProjectionResponse {
    pub accepted_indices: Vec<i64>,
    pub bind_result: Option<Bytes>,
}
pub struct ProjectionProposal {
    pub bind_call: BindRequest,
    pub bind_result: BindResponse,
    pub source_schema: SchemaRef,
    pub result_schema: SchemaRef,
    pub expressions: Vec<SqlExpression>,
}
pub struct ProjectionAcceptance {
    pub accepted_indices: Vec<i64>,
    pub bind_result: crate::function::BindResponse,
}
fn invalid(s: &str) -> RpcError {
    RpcError::value_error(s)
}
pub(crate) fn validate_offer(profiles: &[String]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    if profiles.len() > 64 {
        return Err(invalid("invalid projection profile offer"));
    }
    for p in profiles {
        let mut parts = p.rsplitn(2, ".v");
        let version = parts.next().unwrap_or("");
        let prefix = parts.next().unwrap_or("");
        if p.len() > 256
            || !seen.insert(p)
            || version.is_empty()
            || version.starts_with('0')
            || !version.bytes().all(|c| c.is_ascii_digit())
            || prefix.is_empty()
            || !prefix.split('.').all(|part| {
                part.as_bytes()
                    .first()
                    .is_some_and(|c| c.is_ascii_lowercase())
                    && part
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
            })
        {
            return Err(invalid("invalid projection profile offer"));
        }
    }
    Ok(())
}
pub(crate) fn record(bytes: &[u8], expected: &Schema) -> Result<arrow_array::RecordBatch> {
    if bytes.is_empty() || bytes.len() > 8 * 1024 * 1024 {
        return Err(invalid("invalid projection record size"));
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let row = {
        let mut reader = arrow_ipc::reader::StreamReader::try_new(&mut cursor, None)
            .map_err(|e| invalid(&e.to_string()))?;
        if reader.schema().as_ref() != expected {
            return Err(invalid("invalid projection record schema"));
        }
        let row = reader
            .next()
            .ok_or_else(|| invalid("missing projection row"))?
            .map_err(|e| invalid(&e.to_string()))?;
        if row.num_rows() != 1 {
            return Err(invalid("projection requires exactly one row"));
        }
        for column in row.columns() {
            column
                .to_data()
                .validate_full()
                .map_err(|e| invalid(&e.to_string()))?;
        }
        if reader.next().is_some() {
            return Err(invalid("projection requires exactly one IPC batch"));
        }
        row
    };
    if cursor.position() != bytes.len() as u64 {
        return Err(invalid("trailing projection IPC"));
    }
    Ok(row)
}
impl ProjectionProposal {
    pub(crate) fn decode(r: ProjectionRequest) -> Result<Self> {
        if r.bind_call.0.len()
            + r.bind_result.0.len()
            + r.result_schema.0.len()
            + r.expressions.iter().map(|v| v.0.len()).sum::<usize>()
            > 8 * 1024 * 1024
        {
            return Err(invalid("projection exceeds 8MiB"));
        }
        if ipc::write_batch(&wire::to_batch(r.clone())?)?.len() > 8 * 1024 * 1024 {
            return Err(invalid("projection exceeds 8MiB"));
        }
        let bind_call: BindRequest = wire::from_batch(&record(
            &r.bind_call.0,
            &vgi_protocol::generated::protocol_schemas::bind_request_schema(),
        )?)?;
        let bind_result: BindResponse = wire::from_batch(&record(
            &r.bind_result.0,
            &vgi_protocol::generated::protocol_schemas::bind_result_schema(),
        )?)?;
        validate_offer(&bind_result.projection_expression_profiles)?;
        if !bind_call.function_type.0.eq_ignore_ascii_case("table")
            || bind_call.input_schema.is_some()
            || bind_call.schema_path.is_none()
        {
            return Err(invalid(
                "projection requires an ordinary table and explicit owner",
            ));
        }
        if !bind_result.lookup_names.is_empty()
            || !bind_result.lookup_scopes.is_empty()
            || !bind_result.lookup_secret_types.is_empty()
        {
            return Err(invalid("projection cannot request secrets"));
        }
        let source_schema = ipc::read_schema(&bind_result.output_schema.0)?;
        let result_schema = ipc::read_schema(&r.result_schema.0)?;
        if r.expressions.is_empty()
            || r.expressions.len() > 1024
            || result_schema.fields().len() != r.expressions.len()
            || !result_schema.metadata().is_empty()
        {
            return Err(invalid("invalid projection result schema/count"));
        }
        let mut names = std::collections::HashSet::new();
        for f in source_schema.fields().iter().chain(result_schema.fields()) {
            if f.name().is_empty()
                || f.name().contains('\0')
                || !names.insert(f.name().to_ascii_lowercase())
            {
                return Err(invalid("ambiguous projection column names"));
            }
        }
        let mut expressions = Vec::new();
        let mut context = None;
        for raw in r.expressions {
            crate::column_expressions::encoded(
                &base64::engine::general_purpose::STANDARD.encode(&raw.0),
                bind_call.schema_path.as_ref().unwrap(),
            )?;
            let e: SqlExpression = wire::from_batch(&ipc::read_batch(&raw.0)?)?;
            if !bind_result
                .projection_expression_profiles
                .contains(&e.profile)
            {
                return Err(invalid("projection profile was not offered"));
            }
            let next = format!("{:?}", e.evaluation_context);
            if context.as_ref().is_some_and(|v| v != &next) {
                return Err(invalid("projection contexts differ"));
            }
            context = Some(next);
            expressions.push(e);
        }
        Ok(Self {
            bind_call,
            bind_result,
            source_schema,
            result_schema,
            expressions,
        })
    }
    pub(crate) fn response(
        &self,
        accepted: Option<ProjectionAcceptance>,
    ) -> Result<ProjectionResponse> {
        let Some(a) = accepted else {
            return Ok(ProjectionResponse {
                accepted_indices: Vec::new(),
                bind_result: None,
            });
        };
        if a.accepted_indices.is_empty() {
            return Err(invalid("accepted bind result requires indices"));
        }
        let mut previous = -1;
        let mut fields = self.source_schema.fields().to_vec();
        for i in &a.accepted_indices {
            if *i <= previous || *i as usize >= self.expressions.len() {
                return Err(invalid("invalid accepted projection index"));
            }
            previous = *i;
            fields.push(self.result_schema.fields()[*i as usize].clone());
        }
        let expected = Schema::new_with_metadata(fields, self.source_schema.metadata().clone());
        if a.bind_result.output_schema.as_ref() != &expected {
            return Err(invalid("accepted projection changed append-only schema"));
        }
        let result = BindResponse {
            output_schema: Bytes::from(ipc::write_schema_ref(&Arc::new(expected))?),
            opaque_data: Some(Bytes::from(a.bind_result.opaque_data)),
            lookup_secret_types: Vec::new(),
            lookup_scopes: Vec::new(),
            lookup_names: Vec::new(),
            projection_expression_profiles: self.bind_result.projection_expression_profiles.clone(),
            repeatability: self.bind_result.repeatability,
        };
        Ok(ProjectionResponse {
            accepted_indices: a.accepted_indices,
            bind_result: Some(Bytes::from(ipc::write_batch(&wire::to_batch(result)?)?)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use vgi_rpc::DictString;
    fn proposal() -> ProjectionProposal {
        let metadata = std::collections::HashMap::from([("source".into(), "keep".into())]);
        let source_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("n", DataType::Int32, true)],
            metadata,
        ));
        ProjectionProposal {
            bind_call: BindRequest {
                function_name: "test".into(),
                arguments: Bytes::from(Vec::new()),
                function_type: DictString("TABLE".into()),
                input_schema: None,
                settings: None,
                secrets: None,
                attach_opaque_data: None,
                transaction_opaque_data: None,
                resolved_secrets_provided: false,
                at_unit: None,
                at_value: None,
                schema_path: Some(vec!["main".into()]),
                argument_names: None,
            },
            bind_result: BindResponse {
                output_schema: Bytes::from(Vec::new()),
                opaque_data: None,
                lookup_secret_types: Vec::new(),
                lookup_scopes: Vec::new(),
                lookup_names: Vec::new(),
                projection_expression_profiles: vec![PROFILE.into()],
                repeatability: crate::table_function::BindRepeatability::StableWithinQuery,
            },
            source_schema,
            result_schema: Arc::new(Schema::new(vec![Field::new("wide", DataType::Int64, true)
                .with_metadata(std::collections::HashMap::from([(
                    "result".into(),
                    "keep".into(),
                )]))])),
            expressions: vec![wire::from_batch(
                &ipc::read_batch(
                    &base64::engine::general_purpose::STANDARD
                        .decode(
                            crate::column_expressions::generated_metadata(
                                "CAST(\"n\" AS BIGINT)",
                                &["main".into()],
                            )
                            .unwrap(),
                        )
                        .unwrap(),
                )
                .unwrap(),
            )
            .unwrap()],
        }
    }
    #[test]
    fn projection_provider_schema_and_indices_are_authoritative() {
        let p = proposal();
        let mut fields = p.source_schema.fields().to_vec();
        fields.push(p.result_schema.fields()[0].clone());
        let correct = Arc::new(Schema::new_with_metadata(
            fields.clone(),
            p.source_schema.metadata().clone(),
        ));
        let accepted = |indices, schema| {
            Some(ProjectionAcceptance {
                accepted_indices: indices,
                bind_result: crate::function::BindResponse {
                    output_schema: schema,
                    opaque_data: vec![7],
                },
            })
        };
        assert!(p.response(None).unwrap().bind_result.is_none());
        let accepted_wire = p
            .response(accepted(vec![0], correct.clone()))
            .unwrap()
            .bind_result
            .unwrap();
        let accepted_bind: BindResponse =
            wire::from_batch(&ipc::read_batch(&accepted_wire.0).unwrap()).unwrap();
        assert_eq!(accepted_bind.repeatability, p.bind_result.repeatability);
        for indices in [vec![], vec![-1], vec![1], vec![0, 0]] {
            assert!(p.response(accepted(indices, correct.clone())).is_err());
        }
        assert!(p
            .response(accepted(vec![0], p.source_schema.clone()))
            .is_err());
        assert!(p
            .response(accepted(vec![0], Arc::new(Schema::new(fields))))
            .is_err());
    }
    #[test]
    fn projection_offer_supports_unknown_versioned_identity() {
        assert!(validate_offer(&[PROFILE.into(), "vendor.scalar.v7".into()]).is_ok());
        for offer in [
            vec!["bad".into()],
            vec!["vendor.scalar.v0".into()],
            vec![PROFILE.into(), PROFILE.into()],
        ] {
            assert!(validate_offer(&offer).is_err());
        }
    }
}
