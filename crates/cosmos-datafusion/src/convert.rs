//! JSON ↔ Arrow helpers for the DataFusion federation layer.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::reader::{ReaderBuilder, infer_json_schema_from_iterator};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use serde_json::Value;

/// Infer an Arrow schema from sampled documents.
pub(crate) fn infer_schema(docs: &[Value]) -> Result<SchemaRef, ArrowError> {
    let schema = infer_json_schema_from_iterator(docs.iter().map(Ok::<_, ArrowError>))?;
    Ok(Arc::new(schema))
}

/// Project documents into a known Arrow schema.
pub(crate) fn decode_docs(schema: SchemaRef, docs: &[Value]) -> Result<Vec<RecordBatch>, ArrowError> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
    decoder.serialize(docs)?;
    Ok(decoder.flush()?.into_iter().collect())
}

/// Build the projected output schema and the Cosmos SQL for a scan.
///
/// Projection is pushed into the SELECT list (`SELECT c["f"] AS f, … FROM c`) and any
/// limit becomes `OFFSET 0 LIMIT n`. Filters are not pushed yet — DataFusion applies them
/// locally — so results stay correct while federation (joins/aggregates) is proven out.
pub(crate) fn build_scan_sql(
    full_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> (SchemaRef, String) {
    let (schema, select) = match projection {
        Some(proj) => {
            let fields: Vec<Field> = proj.iter().map(|&i| full_schema.field(i).clone()).collect();
            let cols: Vec<String> = fields
                .iter()
                .map(|f| {
                    let name = f.name();
                    format!("c[\"{}\"] AS {}", name.replace('"', "\\\""), name)
                })
                .collect();
            (Arc::new(Schema::new(fields)), cols.join(", "))
        }
        None => (full_schema.clone(), "*".to_string()),
    };

    let mut sql = format!("SELECT {select} FROM c");
    if let Some(n) = limit {
        sql.push_str(&format!(" OFFSET 0 LIMIT {n}"));
    }
    (schema, sql)
}
