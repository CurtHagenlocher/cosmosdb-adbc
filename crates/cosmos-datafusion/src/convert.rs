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
