//! JSON ↔ Arrow helpers for the DataFusion federation layer.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_json::reader::{ReaderBuilder, infer_json_schema_from_iterator};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use serde_json::Value;

use crate::normalize;

/// Infer an Arrow schema from sampled documents, tolerant of type-conflicting fields at any depth.
///
/// `infer_json_schema_from_iterator` errors when a path is a scalar in one document and an
/// object/array in another, so we first stringify the conflicting nodes ([`normalize`]) — they
/// become `Utf8`; homogeneous nested `Struct`/`List` typing is preserved.
pub(crate) fn infer_schema(docs: &[Value]) -> Result<SchemaRef, ArrowError> {
    let shape = normalize::infer_doc_shape(docs);
    let normalized: Vec<Value> =
        docs.iter().map(|d| shape.normalize(d, &HashSet::new())).collect();
    let schema = infer_json_schema_from_iterator(normalized.iter().map(Ok::<_, ArrowError>))?;
    Ok(Arc::new(schema))
}

/// Project documents into a known Arrow schema. Values are stringified wherever the schema types a
/// field as `Utf8` (at any depth) — so conflicting fields (inferred as `Utf8`) and out-of-sample
/// type drift don't fail the decode.
pub(crate) fn decode_docs(schema: SchemaRef, docs: &[Value]) -> Result<Vec<RecordBatch>, ArrowError> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    let normalized = normalize::coerce_to_schema(&schema, docs);
    let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
    decoder.serialize(&normalized)?;
    Ok(decoder.flush()?.into_iter().collect())
}

/// Decode a `SELECT VALUE <aggregate>` response into a single-row batch matching `schema`.
///
/// The response is at most one bare scalar (Cosmos returns `[n]` for `COUNT`, `[avg]` for
/// `AVG`, or `[]` when the aggregate is undefined — e.g. `AVG` over no numeric values).
/// `schema` has exactly one field whose type was copied from the DataFusion aggregate output,
/// so the fold reproduces that node's schema exactly:
/// - `Int64` (COUNT): missing → `0`, matching COUNT over an empty set.
/// - `Float64` (AVG): missing → `null`, matching AVG over an empty set.
pub(crate) fn decode_scalar_agg(
    schema: SchemaRef,
    docs: &[Value],
) -> Result<Vec<RecordBatch>, ArrowError> {
    let value = docs.first();
    let field = schema.field(0);
    let array: ArrayRef = match field.data_type() {
        DataType::Int64 => {
            Arc::new(Int64Array::from(vec![value.and_then(Value::as_i64).unwrap_or(0)]))
        }
        DataType::Float64 => Arc::new(Float64Array::from(vec![value.and_then(Value::as_f64)])),
        other => {
            return Err(ArrowError::SchemaError(format!(
                "unsupported aggregate output type {other:?}"
            )));
        }
    };
    Ok(vec![RecordBatch::try_new(schema, vec![array])?])
}

/// Build the projected output schema and the Cosmos SQL for a scan.
///
/// Projection is pushed into the SELECT list (`SELECT c["f"] AS f, … FROM c`), pushable
/// filters become a `WHERE` clause (see [`crate::predicate`]), and any limit becomes
/// `OFFSET 0 LIMIT n`. Filters DataFusion did *not* mark pushable never reach here — it
/// applies those locally.
pub(crate) fn build_scan_sql(
    full_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    where_clause: Option<&str>,
    limit: Option<usize>,
) -> (SchemaRef, String) {
    let (schema, select) = match projection {
        Some(proj) => {
            let fields: Vec<Field> = proj.iter().map(|&i| full_schema.field(i).clone()).collect();
            let cols: Vec<String> = fields
                .iter()
                .map(|f| {
                    let name = f.name();
                    format!("{} AS {}", crate::predicate::cosmos_property(name), name)
                })
                .collect();
            (Arc::new(Schema::new(fields)), cols.join(", "))
        }
        None => (full_schema.clone(), "*".to_string()),
    };

    let mut sql = format!("SELECT {select} FROM c");
    if let Some(clause) = where_clause {
        sql.push_str(&format!(" WHERE {clause}"));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" OFFSET 0 LIMIT {n}"));
    }
    (schema, sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("mergeOrder", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn projection_where_and_limit_compose() {
        let s = schema();
        let (proj_schema, sql) = build_scan_sql(
            &s,
            Some(&vec![0, 1]),
            Some(r#"(IS_DEFINED(c["mergeOrder"]) AND NOT IS_NULL(c["mergeOrder"]) AND (c["mergeOrder"] > 25))"#),
            Some(10),
        );
        assert_eq!(
            sql,
            r#"SELECT c["id"] AS id, c["mergeOrder"] AS mergeOrder FROM c WHERE (IS_DEFINED(c["mergeOrder"]) AND NOT IS_NULL(c["mergeOrder"]) AND (c["mergeOrder"] > 25)) OFFSET 0 LIMIT 10"#
        );
        // The projected schema carries only the selected columns, in order.
        let names: Vec<&str> = proj_schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "mergeOrder"]);
    }

    #[test]
    fn no_projection_no_filter_is_select_star() {
        let (_, sql) = build_scan_sql(&schema(), None, None, None);
        assert_eq!(sql, "SELECT * FROM c");
    }

    #[test]
    fn where_clause_precedes_offset_limit() {
        let (_, sql) = build_scan_sql(&schema(), None, Some("(c[\"a\"] = 1)"), Some(5));
        assert_eq!(sql, "SELECT * FROM c WHERE (c[\"a\"] = 1) OFFSET 0 LIMIT 5");
    }
}
