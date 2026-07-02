//! JSON ↔ Arrow helpers for the DataFusion federation layer.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::reader::{ReaderBuilder, infer_json_schema_from_iterator};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use serde_json::{Map, Value};

/// Infer an Arrow schema from sampled documents, tolerant of type-conflicting fields.
///
/// `infer_json_schema_from_iterator` errors when a field is a scalar in one document and an
/// object/array in another, so we detect heterogeneous top-level fields up front, exclude them
/// from inference, and represent them as `Utf8` (decoded via [`decode_docs`]'s stringify path).
pub(crate) fn infer_schema(docs: &[Value]) -> Result<SchemaRef, ArrowError> {
    let heterogeneous = heterogeneous_fields(docs);
    let base = if heterogeneous.is_empty() {
        infer_json_schema_from_iterator(docs.iter().map(Ok::<_, ArrowError>))?
    } else {
        let reduced: Vec<Value> = docs.iter().map(|d| without_fields(d, &heterogeneous)).collect();
        infer_json_schema_from_iterator(reduced.iter().map(Ok::<_, ArrowError>))?
    };

    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut het: Vec<&String> = heterogeneous.iter().collect();
    het.sort();
    for name in het {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Project documents into a known Arrow schema. For any field the schema types as `Utf8`, a
/// non-string JSON value is stringified first — so heterogeneous fields (inferred as `Utf8`) and
/// out-of-sample type drift don't fail the decode.
pub(crate) fn decode_docs(schema: SchemaRef, docs: &[Value]) -> Result<Vec<RecordBatch>, ArrowError> {
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    let utf8_fields: HashSet<&str> = schema
        .fields()
        .iter()
        .filter(|f| matches!(f.data_type(), DataType::Utf8))
        .map(|f| f.name().as_str())
        .collect();
    let processed: Cow<[Value]> = if utf8_fields.is_empty() {
        Cow::Borrowed(docs)
    } else {
        Cow::Owned(docs.iter().map(|d| stringify_fields(d, &utf8_fields)).collect())
    };

    let mut decoder = ReaderBuilder::new(schema).build_decoder()?;
    decoder.serialize(&processed)?;
    Ok(decoder.flush()?.into_iter().collect())
}

/// Top-level fields whose sampled values take more than one JSON kind (integers and floats both
/// count as numbers, so a numeric field is never flagged).
fn heterogeneous_fields(docs: &[Value]) -> HashSet<String> {
    let mut kinds: HashMap<String, HashSet<u8>> = HashMap::new();
    for doc in docs {
        if let Value::Object(map) = doc {
            for (k, v) in map {
                if let Some(kind) = json_kind(v) {
                    kinds.entry(k.clone()).or_default().insert(kind);
                }
            }
        }
    }
    kinds.into_iter().filter(|(_, ks)| ks.len() > 1).map(|(k, _)| k).collect()
}

fn json_kind(v: &Value) -> Option<u8> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(0),
        Value::Number(_) => Some(1),
        Value::String(_) => Some(2),
        Value::Array(_) => Some(3),
        Value::Object(_) => Some(4),
    }
}

fn without_fields(doc: &Value, exclude: &HashSet<String>) -> Value {
    match doc {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(k, _)| !exclude.contains(k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Stringify non-string values for the named fields (numbers → `"123"`, objects/arrays → JSON).
fn stringify_fields(doc: &Value, fields: &HashSet<&str>) -> Value {
    match doc {
        Value::Object(map) => {
            let out: Map<String, Value> = map
                .iter()
                .map(|(k, v)| {
                    let nv = if fields.contains(k.as_str()) {
                        match v {
                            Value::Null | Value::String(_) => v.clone(),
                            other => Value::String(other.to_string()),
                        }
                    } else {
                        v.clone()
                    };
                    (k.clone(), nv)
                })
                .collect();
            Value::Object(out)
        }
        other => other.clone(),
    }
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
