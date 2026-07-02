//! Recursive JSON normalization that keeps arrow-json's inference and decoding from crashing on
//! **type-conflicting fields at any depth**.
//!
//! `infer_json_schema_from_iterator` errors when a path is a scalar in one document and an
//! object/array in another; and even for scalar-vs-scalar (e.g. int vs string) its `Decoder`
//! rejects the values it inferred as `Utf8`. We fix both by, before inference/decoding,
//! stringifying exactly the *conflicting* nodes (turning them into homogeneous `Utf8`), guided by
//! a merged **shape** computed across the sampled documents. Non-conflicting fields are untouched,
//! so nested `Struct`/`List` typing is preserved.

use std::collections::{BTreeMap, HashSet};

use arrow_schema::{DataType, Fields, Schema};
use serde_json::{Map, Value};

/// The merged JSON shape of a field across documents. Integers and floats are both `Number`, so a
/// numeric field is never a conflict; a field seen as two different kinds becomes `Conflict`.
#[derive(Debug, Clone)]
enum Shape {
    /// Only null / absent seen — compatible with anything.
    Unknown,
    Bool,
    Number,
    Str,
    Object(BTreeMap<String, Shape>),
    Array(Box<Shape>),
    /// Incompatible kinds seen at this node — normalize to a `Utf8` string.
    Conflict,
}

/// The merged shape of a batch of documents (a root [`Shape`]), used to normalize them.
#[derive(Debug, Clone)]
pub struct DocShape(Shape);

/// Merge the shapes of every document into one.
pub fn infer_doc_shape(docs: &[Value]) -> DocShape {
    let mut shape = Shape::Unknown;
    for doc in docs {
        shape = merge(shape, shape_of(doc));
    }
    DocShape(shape)
}

impl DocShape {
    /// Top-level field names whose values conflict across documents (used to route them to a
    /// Variant column instead of stringifying).
    pub fn top_level_conflicts(&self) -> Vec<String> {
        match &self.0 {
            Shape::Object(fields) => fields
                .iter()
                .filter(|(_, s)| matches!(s, Shape::Conflict))
                .map(|(k, _)| k.clone())
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Return a copy of `doc` with every conflicting node stringified, except top-level fields in
    /// `skip_top` (left raw — e.g. fields handled as Variant columns).
    pub fn normalize(&self, doc: &Value, skip_top: &HashSet<&str>) -> Value {
        normalize_value(doc, &self.0, Some(skip_top))
    }
}

fn shape_of(v: &Value) -> Shape {
    match v {
        Value::Null => Shape::Unknown,
        Value::Bool(_) => Shape::Bool,
        Value::Number(_) => Shape::Number,
        Value::String(_) => Shape::Str,
        Value::Object(map) => {
            Shape::Object(map.iter().map(|(k, v)| (k.clone(), shape_of(v))).collect())
        }
        Value::Array(items) => {
            let mut element = Shape::Unknown;
            for item in items {
                element = merge(element, shape_of(item));
            }
            Shape::Array(Box::new(element))
        }
    }
}

fn merge(a: Shape, b: Shape) -> Shape {
    use Shape::*;
    match (a, b) {
        (Unknown, x) | (x, Unknown) => x,
        (Conflict, _) | (_, Conflict) => Conflict,
        (Bool, Bool) => Bool,
        (Number, Number) => Number,
        (Str, Str) => Str,
        (Object(mut m1), Object(m2)) => {
            for (k, v) in m2 {
                let existing = m1.remove(&k).unwrap_or(Shape::Unknown);
                m1.insert(k, merge(existing, v));
            }
            Object(m1)
        }
        (Array(e1), Array(e2)) => Array(Box::new(merge(*e1, *e2))),
        _ => Conflict,
    }
}

fn normalize_value(v: &Value, shape: &Shape, skip_top: Option<&HashSet<&str>>) -> Value {
    match shape {
        Shape::Conflict => stringify(v),
        Shape::Object(fields) => {
            let Value::Object(obj) = v else {
                return v.clone(); // e.g. null where the merged shape is an object
            };
            let out: Map<String, Value> = obj
                .iter()
                .map(|(k, val)| {
                    if skip_top.is_some_and(|s| s.contains(k.as_str())) {
                        return (k.clone(), val.clone()); // left raw (Variant field)
                    }
                    let sub = fields.get(k).unwrap_or(&Shape::Unknown);
                    (k.clone(), normalize_value(val, sub, None))
                })
                .collect();
            Value::Object(out)
        }
        Shape::Array(element) => {
            let Value::Array(items) = v else {
                return v.clone();
            };
            Value::Array(items.iter().map(|it| normalize_value(it, element, None)).collect())
        }
        Shape::Bool | Shape::Number | Shape::Str | Shape::Unknown => v.clone(),
    }
}

/// Stringify a JSON value (numbers → `"123"`, objects/arrays → compact JSON); nulls stay null.
fn stringify(v: &Value) -> Value {
    match v {
        Value::Null | Value::String(_) => v.clone(),
        other => Value::String(other.to_string()),
    }
}

/// Schema-guided normalization for decoding: stringify values wherever the *target schema* types a
/// field as `Utf8`, at any depth. Complements [`DocShape::normalize`] for callers that infer the
/// schema separately from decoding (the DataFusion provider), and also tolerates out-of-sample
/// type drift.
pub fn coerce_to_schema(schema: &Schema, docs: &[Value]) -> Vec<Value> {
    docs.iter().map(|d| coerce_struct(d, schema.fields())).collect()
}

fn coerce_struct(v: &Value, fields: &Fields) -> Value {
    let Value::Object(obj) = v else {
        return v.clone();
    };
    let out: Map<String, Value> = obj
        .iter()
        .map(|(k, val)| match fields.iter().find(|f| f.name() == k) {
            Some(f) => (k.clone(), coerce_value(val, f.data_type())),
            None => (k.clone(), val.clone()),
        })
        .collect();
    Value::Object(out)
}

fn coerce_value(v: &Value, dt: &DataType) -> Value {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => stringify(v),
        DataType::Struct(fields) => coerce_struct(v, fields),
        DataType::List(field) | DataType::LargeList(field) => {
            let Value::Array(items) = v else {
                return v.clone();
            };
            Value::Array(items.iter().map(|it| coerce_value(it, field.data_type())).collect())
        }
        _ => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_scalar_conflict_stringified() {
        let docs = vec![json!({"a": {"b": 1}}), json!({"a": {"b": "x"}})];
        let shape = infer_doc_shape(&docs);
        let n0 = shape.normalize(&docs[0], &HashSet::new());
        assert_eq!(n0, json!({"a": {"b": "1"}}));
        assert!(shape.top_level_conflicts().is_empty(), "conflict is nested, not top-level");
    }

    #[test]
    fn nested_scalar_vs_object_stringified() {
        let docs = vec![json!({"a": {"b": 1}}), json!({"a": {"b": {"c": 1}}})];
        let shape = infer_doc_shape(&docs);
        assert_eq!(shape.normalize(&docs[1], &HashSet::new()), json!({"a": {"b": "{\"c\":1}"}}));
    }

    #[test]
    fn array_element_conflict_stringified() {
        let docs = vec![json!({"t": [1, 2]}), json!({"t": ["a"]})];
        let shape = infer_doc_shape(&docs);
        assert_eq!(shape.normalize(&docs[0], &HashSet::new()), json!({"t": ["1", "2"]}));
    }

    #[test]
    fn homogeneous_nesting_untouched() {
        let docs = vec![json!({"o": {"k": 1}, "t": [1, 2]}), json!({"o": {"k": 2}, "t": [3]})];
        let shape = infer_doc_shape(&docs);
        assert_eq!(shape.normalize(&docs[0], &HashSet::new()), docs[0]);
    }

    #[test]
    fn top_level_conflict_reported_and_skippable() {
        let docs = vec![json!({"v": 1}), json!({"v": "x"})];
        let shape = infer_doc_shape(&docs);
        assert_eq!(shape.top_level_conflicts(), vec!["v".to_string()]);
        // skipped → left raw
        let skip: HashSet<&str> = ["v"].into_iter().collect();
        assert_eq!(shape.normalize(&docs[0], &skip), json!({"v": 1}));
        // not skipped → stringified
        assert_eq!(shape.normalize(&docs[0], &HashSet::new()), json!({"v": "1"}));
    }
}
