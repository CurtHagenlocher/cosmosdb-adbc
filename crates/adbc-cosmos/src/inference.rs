//! Schema inference for the `struct` output mode (DESIGN §3.5).
//!
//! Base inference is arrow-json's `infer_json_schema_from_iterator` (Int64/Float64/Boolean/
//! Utf8/Struct/List). Type-conflicting fields at **any depth** are first normalized to `Utf8`
//! strings via the shared [`cosmos_datafusion::normalize`] shape pass (arrow-json's infer/decode
//! otherwise crash on them); then a small, opt-in set of **top-level** type transforms
//! ([`InferenceOptions`]) is applied and arrow-json's `Decoder` coerces the documents in (it
//! natively decodes JSON numbers into `Decimal128`, RFC-3339 strings into `Timestamp`/`Date32`,
//! and integers into `Timestamp`). A top-level conflicting field can instead be carried as a
//! self-describing Variant column (`heterogeneous=variant`, `variant` feature).
//!
//! `number_inference=decimal` and `infer_temporal` recurse into nested `Struct`/`List` types;
//! `epoch_fields` matches top-level field names only. Conflict normalization is fully recursive.
//! All knobs default off/float64 so behavior matches plain inference until a user opts in.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::RecordBatch;
use arrow_json::reader::{ReaderBuilder, infer_json_schema_from_iterator};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use cosmos_datafusion::normalize::{self, DocShape};
use driverbase::error::ErrorHelper as _;
use serde_json::Value;

use crate::error::ErrorHelper;

/// How JSON numbers with a fractional part are typed (integers are always exact `Int64`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NumberMode {
    /// `Float64` (default) — fast, analytics-friendly, lossy past ~15 digits.
    #[default]
    Float64,
    /// `Decimal128(precision, scale)` — precise; applied to fractional (non-integer) fields.
    Decimal { precision: u8, scale: i8 },
}

/// How a field whose sampled documents disagree on type is represented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HeterogeneousMode {
    /// Widen to `Utf8`, stringifying non-string values (universally consumable). Default and
    /// the only mode available without the `variant` feature.
    #[default]
    String,
    /// Encode the field as a self-describing Arrow **Variant** column (lossless). Requires the
    /// `variant` feature.
    #[cfg(feature = "variant")]
    Variant,
}

/// Options controlling `struct`-mode inference. `Default` reproduces plain arrow-json.
#[derive(Debug, Clone, Default)]
pub struct InferenceOptions {
    pub number: NumberMode,
    pub infer_temporal: bool,
    /// Field name → epoch unit; the raw integer is read as the timestamp value in that unit.
    pub epoch_fields: HashMap<String, TimeUnit>,
    /// Only read when the `variant` feature is on (otherwise heterogeneous fields are always
    /// carried as `Utf8`).
    #[cfg_attr(not(feature = "variant"), allow(dead_code))]
    pub heterogeneous: HeterogeneousMode,
}

fn arrow_err(context: &'static str, e: ArrowError) -> adbc_core::error::Error {
    ErrorHelper::internal(context).message(e.to_string()).to_adbc()
}

/// Build a `struct`-mode batch. Type-conflicting fields at any depth are stringified to `Utf8`
/// by the shared [`normalize`] shape pass before inference/decoding (nested conflicts included);
/// a top-level conflicting field is instead carried as a self-describing Variant column under
/// `heterogeneous=variant`. Homogeneous fields are inferred, top-level type-transformed (§3.5),
/// and decoded normally.
pub fn build_struct_batch(
    docs: &[Value],
    sample_size: usize,
    opts: &InferenceOptions,
) -> Result<RecordBatch> {
    if docs.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    let sample_n = sample_size.max(1);
    let sample = &docs[..docs.len().min(sample_n)];
    let shape = normalize::infer_doc_shape(sample);

    // Top-level conflicting fields become Variant columns (heterogeneous=variant + feature); every
    // other conflict — nested, or top-level in string mode — is stringified inline by `normalize`.
    let variant_fields = variant_fields(opts, &shape);
    let skip: std::collections::HashSet<&str> = variant_fields.iter().map(String::as_str).collect();
    let normalized: Vec<Value> = docs.iter().map(|d| shape.normalize(d, &skip)).collect();

    // Infer over the normalized sample. Variant fields are left raw by `normalize`, so drop them
    // (they'd still be conflicting) — they're rebuilt from the raw documents below.
    let sample_end = normalized.len().min(sample_n);
    let base = if variant_fields.is_empty() {
        infer_json_schema_from_iterator(normalized[..sample_end].iter().map(Ok::<_, ArrowError>))
    } else {
        let reduced: Vec<Value> =
            normalized[..sample_end].iter().map(|d| without_fields(d, &skip)).collect();
        infer_json_schema_from_iterator(reduced.iter().map(Ok::<_, ArrowError>))
    }
    .map_err(|e| arrow_err("infer_json_schema", e))?;

    // Apply §3.5 type transforms and decode the normalized documents (variant fields aren't in the
    // schema, so the Decoder ignores them). `decimal`/`infer_temporal` recurse into nested
    // structs/lists; `epoch_fields` matches top-level field names only.
    let root = build_node_profile(&normalized[..sample_end]);
    let decode_fields: Vec<Field> = base
        .fields()
        .iter()
        .map(|f| {
            let dt = if let Some(unit) = opts.epoch_fields.get(f.name()) {
                DataType::Timestamp(*unit, None)
            } else {
                transform_type(f.data_type(), root.fields.get(f.name()), opts)
            };
            Field::new(f.name(), dt, true)
        })
        .collect();
    let decode_schema = Arc::new(Schema::new(decode_fields));
    let typed_batch = if decode_schema.fields().is_empty() {
        RecordBatch::new_empty(decode_schema.clone())
    } else {
        decode(decode_schema, &normalized)?
    };

    if variant_fields.is_empty() {
        return Ok(typed_batch);
    }
    #[cfg(feature = "variant")]
    {
        assemble_variants(typed_batch, docs, &variant_fields)
    }
    #[cfg(not(feature = "variant"))]
    {
        Ok(typed_batch) // unreachable: variant_fields is empty without the feature
    }
}

/// Infer an Arrow schema tolerant of type-conflicting fields at any depth, for metadata
/// (`get_table_schema`, `get_objects` columns). Conflicting fields are normalized to `Utf8`; no
/// §3.5 transforms (those are query-output knobs). Returns the raw `ArrowError` for the caller.
pub fn infer_schema(
    docs: &[Value],
    sample_size: usize,
) -> std::result::Result<Arc<Schema>, ArrowError> {
    if docs.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }
    let sample_n = sample_size.max(1);
    let sample = &docs[..docs.len().min(sample_n)];
    let shape = normalize::infer_doc_shape(sample);
    let no_skip = std::collections::HashSet::new();
    let normalized: Vec<Value> = sample.iter().map(|d| shape.normalize(d, &no_skip)).collect();
    let schema = infer_json_schema_from_iterator(normalized.iter().map(Ok::<_, ArrowError>))?;
    Ok(Arc::new(schema))
}

/// Top-level conflicting fields to carry as Variant columns (under `heterogeneous=variant`).
/// Always empty without the `variant` feature.
#[cfg(feature = "variant")]
fn variant_fields(opts: &InferenceOptions, shape: &DocShape) -> std::collections::HashSet<String> {
    if opts.heterogeneous == HeterogeneousMode::Variant {
        shape.top_level_conflicts().into_iter().collect()
    } else {
        std::collections::HashSet::new()
    }
}

#[cfg(not(feature = "variant"))]
fn variant_fields(_opts: &InferenceOptions, _shape: &DocShape) -> std::collections::HashSet<String> {
    std::collections::HashSet::new()
}

/// Reassemble the decoded homogeneous columns plus a self-describing Variant column
/// (`json_to_variant`, from the raw documents) for each top-level conflicting field.
#[cfg(feature = "variant")]
fn assemble_variants(
    typed_batch: RecordBatch,
    docs: &[Value],
    variant_fields: &std::collections::HashSet<String>,
) -> Result<RecordBatch> {
    const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
    const ARROW_VARIANT: &str = "arrow.parquet.variant";

    let mut fields: Vec<Field> =
        typed_batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<arrow_array::ArrayRef> = typed_batch.columns().to_vec();

    let mut names: Vec<&String> = variant_fields.iter().collect();
    names.sort();
    for name in names {
        let values: Vec<Option<String>> = docs
            .iter()
            .map(|d| match d.get(name) {
                None | Some(Value::Null) => None,
                Some(v) => Some(v.to_string()),
            })
            .collect();
        let input: arrow_array::ArrayRef = Arc::new(arrow_array::StringArray::from(values));
        let variant_array = parquet_variant_compute::json_to_variant(&input)
            .map_err(|e| arrow_err("json_to_variant", e))?;
        let array: arrow_array::ArrayRef = variant_array.into();
        let metadata = HashMap::from([(EXTENSION_NAME_KEY.to_string(), ARROW_VARIANT.to_string())]);
        fields.push(Field::new(name, array.data_type().clone(), true).with_metadata(metadata));
        columns.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| arrow_err("assemble", e))
}

/// A copy of `doc` with the named top-level fields removed (keeps Variant fields out of
/// arrow-json inference).
fn without_fields(doc: &Value, exclude: &std::collections::HashSet<&str>) -> Value {
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

fn decode(schema: Arc<Schema>, docs: &[Value]) -> Result<RecordBatch> {
    let mut decoder = ReaderBuilder::new(schema.clone())
        .build_decoder()
        .map_err(|e| arrow_err("build_decoder", e))?;
    decoder.serialize(docs).map_err(|e| arrow_err("decode_documents", e))?;
    Ok(decoder
        .flush()
        .map_err(|e| arrow_err("flush", e))?
        .unwrap_or_else(|| RecordBatch::new_empty(schema)))
}

/// Recursively transform an inferred type per the §3.5 knobs: `Float64` → `Decimal128(p,s)` under
/// `number_inference=decimal`, and `Utf8` → `Date32`/`Timestamp` under `infer_temporal` when every
/// sampled value at that path is an ISO-8601 date/datetime (from `profile`). Recurses into
/// `Struct` fields and `List` elements so nested numbers/strings are transformed too.
fn transform_type(dt: &DataType, profile: Option<&NodeProfile>, opts: &InferenceOptions) -> DataType {
    match dt {
        DataType::Float64 => {
            if let NumberMode::Decimal { precision, scale } = opts.number {
                DataType::Decimal128(precision, scale)
            } else {
                dt.clone()
            }
        }
        DataType::Utf8 if opts.infer_temporal => match profile {
            Some(p) if p.all_datetime() => DataType::Timestamp(TimeUnit::Microsecond, None),
            Some(p) if p.all_date() => DataType::Date32,
            _ => dt.clone(),
        },
        DataType::Struct(fields) => {
            let transformed: Vec<Field> = fields
                .iter()
                .map(|f| {
                    let sub = profile.and_then(|p| p.fields.get(f.name()));
                    Field::new(f.name(), transform_type(f.data_type(), sub, opts), f.is_nullable())
                })
                .collect();
            DataType::Struct(transformed.into())
        }
        DataType::List(field) => {
            let sub = profile.and_then(|p| p.element.as_deref());
            let element =
                Field::new(field.name(), transform_type(field.data_type(), sub, opts), field.is_nullable());
            DataType::List(Arc::new(element))
        }
        other => other.clone(),
    }
}

// ── recursive value profiling (for temporal inference at any depth) ──────────

/// Tallies over the sampled values at one JSON node (and, recursively, its object fields and array
/// elements). A `Utf8` node is treated as a date/datetime only if *every* non-null value there is
/// an ISO-8601 date/datetime string.
#[derive(Default)]
struct NodeProfile {
    non_null: usize,
    iso_date: usize,
    iso_datetime: usize,
    fields: HashMap<String, NodeProfile>,
    element: Option<Box<NodeProfile>>,
}

impl NodeProfile {
    fn observe(&mut self, v: &Value) {
        match v {
            Value::Null => {}
            Value::String(s) => {
                if is_iso_datetime(s) {
                    self.iso_datetime += 1;
                } else if is_iso_date(s) {
                    self.iso_date += 1;
                }
                self.non_null += 1;
            }
            Value::Object(map) => {
                self.non_null += 1;
                for (k, val) in map {
                    self.fields.entry(k.clone()).or_default().observe(val);
                }
            }
            Value::Array(items) => {
                self.non_null += 1;
                let element = self.element.get_or_insert_with(Box::default);
                for item in items {
                    element.observe(item);
                }
            }
            _ => self.non_null += 1,
        }
    }

    fn all_datetime(&self) -> bool {
        self.non_null > 0 && self.iso_datetime == self.non_null
    }

    fn all_date(&self) -> bool {
        self.non_null > 0 && self.iso_date == self.non_null
    }
}

/// Build the root profile by observing each document (whose top-level fields land in `.fields`).
fn build_node_profile(sample: &[Value]) -> NodeProfile {
    let mut root = NodeProfile::default();
    for doc in sample {
        root.observe(doc);
    }
    root
}

fn is_iso_date(s: &str) -> bool {
    // YYYY-MM-DD
    let b = s.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..10].iter().all(u8::is_ascii_digit)
}

fn is_iso_datetime(s: &str) -> bool {
    // YYYY-MM-DD, then 'T'/' ', then at least HH:MM:SS (optional fraction / zone left to the decoder).
    let b = s.as_bytes();
    b.len() >= 19
        && is_iso_date(&s[..10])
        && matches!(b[10], b'T' | b't' | b' ')
        && b[13] == b':'
        && b[16] == b':'
        && b[11..13].iter().all(u8::is_ascii_digit)
        && b[14..16].iter().all(u8::is_ascii_digit)
        && b[17..19].iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema_of(docs: &[Value], opts: &InferenceOptions) -> Schema {
        build_struct_batch(docs, 1000, opts).unwrap().schema().as_ref().clone()
    }

    fn dtype<'a>(schema: &'a Schema, name: &str) -> &'a DataType {
        schema.field_with_name(name).unwrap().data_type()
    }

    #[test]
    fn default_matches_plain_inference() {
        let docs = vec![json!({"i": 1, "f": 1.5, "s": "x", "b": true})];
        let s = schema_of(&docs, &InferenceOptions::default());
        assert_eq!(dtype(&s, "i"), &DataType::Int64);
        assert_eq!(dtype(&s, "f"), &DataType::Float64);
        assert_eq!(dtype(&s, "s"), &DataType::Utf8);
        assert_eq!(dtype(&s, "b"), &DataType::Boolean);
    }

    #[test]
    fn decimal_applies_to_floats_not_ints() {
        let docs = vec![json!({"i": 10, "f": 1.25})];
        let opts = InferenceOptions {
            number: NumberMode::Decimal { precision: 38, scale: 9 },
            ..Default::default()
        };
        let s = schema_of(&docs, &opts);
        assert_eq!(dtype(&s, "i"), &DataType::Int64, "integers stay exact");
        assert_eq!(dtype(&s, "f"), &DataType::Decimal128(38, 9));
    }

    #[test]
    fn temporal_inference_opt_in() {
        let docs = vec![json!({"d": "2021-06-15", "dt": "2021-06-15T10:30:00Z", "s": "hello"})];
        // off by default
        let off = schema_of(&docs, &InferenceOptions::default());
        assert_eq!(dtype(&off, "d"), &DataType::Utf8);
        // on
        let opts = InferenceOptions { infer_temporal: true, ..Default::default() };
        let s = schema_of(&docs, &opts);
        assert_eq!(dtype(&s, "d"), &DataType::Date32);
        assert_eq!(dtype(&s, "dt"), &DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(dtype(&s, "s"), &DataType::Utf8, "non-date strings stay Utf8");
    }

    #[test]
    fn epoch_fields_named() {
        let docs = vec![json!({"_ts": 1623752400i64, "ms": 1623752400000i64, "n": 5})];
        let opts = InferenceOptions {
            epoch_fields: HashMap::from([
                ("_ts".to_string(), TimeUnit::Second),
                ("ms".to_string(), TimeUnit::Millisecond),
            ]),
            ..Default::default()
        };
        let s = schema_of(&docs, &opts);
        assert_eq!(dtype(&s, "_ts"), &DataType::Timestamp(TimeUnit::Second, None));
        assert_eq!(dtype(&s, "ms"), &DataType::Timestamp(TimeUnit::Millisecond, None));
        assert_eq!(dtype(&s, "n"), &DataType::Int64, "un-named numbers stay Int64");
    }

    #[test]
    fn heterogeneous_field_widens_to_string_and_decodes() {
        // int in one doc, string in another — plain inference would decode-crash.
        let docs = vec![json!({"het": 123}), json!({"het": "a string"}), json!({"het": true})];
        let batch = build_struct_batch(&docs, 1000, &InferenceOptions::default()).unwrap();
        assert_eq!(dtype(&batch.schema(), "het"), &DataType::Utf8);
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(batch.schema().index_of("het").unwrap())
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .clone();
        assert_eq!(col.value(0), "123");
        assert_eq!(col.value(1), "a string");
        assert_eq!(col.value(2), "true");
    }

    #[test]
    fn homogeneous_object_stays_struct() {
        let docs = vec![json!({"o": {"a": 1}}), json!({"o": {"a": 2}})];
        let s = schema_of(&docs, &InferenceOptions::default());
        assert!(matches!(dtype(&s, "o"), DataType::Struct(_)));
    }

    #[test]
    fn nested_scalar_conflict_becomes_struct_of_utf8() {
        // a.b is int in one doc, string in another — previously decode-crashed.
        let docs = vec![json!({"a": {"b": 1}}), json!({"a": {"b": "x"}})];
        let batch = build_struct_batch(&docs, 1000, &InferenceOptions::default()).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let schema = batch.schema();
        let DataType::Struct(fields) = dtype(&schema, "a") else {
            panic!("a should be a Struct, got {:?}", dtype(&schema, "a"));
        };
        let b = fields.iter().find(|f| f.name() == "b").unwrap();
        assert_eq!(b.data_type(), &DataType::Utf8);
    }

    #[test]
    fn nested_scalar_vs_object_conflict_does_not_crash() {
        // a.b is a scalar in one doc, an object in another — previously an *infer* error.
        let docs = vec![json!({"a": {"b": 1}}), json!({"a": {"b": {"c": 1}}})];
        let batch = build_struct_batch(&docs, 1000, &InferenceOptions::default()).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(matches!(dtype(&batch.schema(), "a"), DataType::Struct(_)));
    }

    #[test]
    fn decimal_recurses_into_nested_struct_and_list() {
        let docs = vec![json!({"o": {"f": 1.5}, "arr": [2.5, 3.5]})];
        let opts = InferenceOptions {
            number: NumberMode::Decimal { precision: 20, scale: 4 },
            ..Default::default()
        };
        let batch = build_struct_batch(&docs, 1000, &opts).unwrap();
        let schema = batch.schema();
        // nested struct field o.f -> Decimal128
        let DataType::Struct(o) = dtype(&schema, "o") else { panic!("o is Struct") };
        assert_eq!(
            o.iter().find(|f| f.name() == "f").unwrap().data_type(),
            &DataType::Decimal128(20, 4)
        );
        // list element -> Decimal128
        let DataType::List(el) = dtype(&schema, "arr") else { panic!("arr is List") };
        assert_eq!(el.data_type(), &DataType::Decimal128(20, 4));
    }

    #[test]
    fn temporal_recurses_into_nested_struct() {
        let docs = vec![
            json!({"o": {"d": "2021-06-15", "dt": "2021-06-15T10:30:00Z"}}),
            json!({"o": {"d": "2022-01-01", "dt": "2022-01-01T00:00:00Z"}}),
        ];
        let opts = InferenceOptions { infer_temporal: true, ..Default::default() };
        let batch = build_struct_batch(&docs, 1000, &opts).unwrap();
        let schema = batch.schema();
        let DataType::Struct(o) = dtype(&schema, "o") else { panic!("o is Struct") };
        assert_eq!(o.iter().find(|f| f.name() == "d").unwrap().data_type(), &DataType::Date32);
        assert_eq!(
            o.iter().find(|f| f.name() == "dt").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn array_element_conflict_becomes_list_of_utf8() {
        let docs = vec![json!({"t": [1, 2]}), json!({"t": ["x"]})];
        let batch = build_struct_batch(&docs, 1000, &InferenceOptions::default()).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let schema = batch.schema();
        let DataType::List(field) = dtype(&schema, "t") else {
            panic!("t should be a List, got {:?}", dtype(&schema, "t"));
        };
        assert_eq!(field.data_type(), &DataType::Utf8);
    }

    #[cfg(feature = "variant")]
    #[test]
    fn heterogeneous_field_as_variant_column() {
        // `het` conflicts (int / string / object); `keep` is a plain typed column.
        let docs = vec![
            json!({"het": 123, "keep": 1}),
            json!({"het": "a string", "keep": 2}),
            json!({"het": {"nested": true}, "keep": 3}),
        ];
        let opts = InferenceOptions {
            heterogeneous: HeterogeneousMode::Variant,
            ..Default::default()
        };
        let batch = build_struct_batch(&docs, 1000, &opts).unwrap();
        assert_eq!(batch.num_rows(), 3);

        // `keep` stays a normal Int64 column.
        assert_eq!(dtype(&batch.schema(), "keep"), &DataType::Int64);

        // `het` is a Variant column: annotated with the canonical extension name and NOT Utf8.
        let het = batch.schema().field_with_name("het").unwrap().clone();
        assert_eq!(
            het.metadata().get("ARROW:extension:name").map(String::as_str),
            Some("arrow.parquet.variant"),
        );
        assert_ne!(het.data_type(), &DataType::Utf8);
    }
}
