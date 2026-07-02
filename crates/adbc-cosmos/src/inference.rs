//! Schema inference for the `struct` output mode (DESIGN §3.5).
//!
//! Base inference is arrow-json's `infer_json_schema_from_iterator` (Int64/Float64/Boolean/
//! Utf8/Struct/List). On top of that we apply a small, opt-in set of type transforms driven
//! by [`InferenceOptions`], then let arrow-json's `Decoder` coerce the documents into the
//! transformed schema (it natively decodes JSON numbers into `Decimal128`, RFC-3339 strings
//! into `Timestamp`/`Date32`, and integers into `Timestamp`, as verified against arrow 58).
//!
//! Transforms are applied to **top-level** fields only; nested numbers/strings keep the base
//! inference for now. All knobs default off/float64 so behavior matches plain inference until
//! a user opts in.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::RecordBatch;
use arrow_json::reader::{ReaderBuilder, infer_json_schema_from_iterator};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use driverbase::error::ErrorHelper as _;
use serde_json::{Map, Value};

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
}

/// Options controlling `struct`-mode inference. `Default` reproduces plain arrow-json.
#[derive(Debug, Clone, Default)]
pub struct InferenceOptions {
    pub number: NumberMode,
    pub infer_temporal: bool,
    /// Field name → epoch unit; the raw integer is read as the timestamp value in that unit.
    pub epoch_fields: HashMap<String, TimeUnit>,
    pub heterogeneous: HeterogeneousMode,
}

fn arrow_err(context: &'static str, e: ArrowError) -> adbc_core::error::Error {
    ErrorHelper::internal(context).message(e.to_string()).to_adbc()
}

/// Build a `struct`-mode batch: profile the sample, transform the inferred schema per `opts`,
/// then decode all documents into it.
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

    let profiles = profile_fields(sample);
    let base = infer_json_schema_from_iterator(sample.iter().map(Ok::<_, ArrowError>))
        .map_err(|e| arrow_err("infer_json_schema", e))?;

    // Transform each top-level field; collect fields we must stringify so a Utf8 decode succeeds.
    let mut fields = Vec::with_capacity(base.fields().len());
    let mut stringify: HashSet<String> = HashSet::new();
    for f in base.fields() {
        let dt = decide_type(f.name(), f.data_type(), profiles.get(f.name()), opts, &mut stringify);
        fields.push(Field::new(f.name(), dt, true));
    }
    let schema = Arc::new(Schema::new(fields));

    // Only clone/rewrite documents if a field needs stringifying.
    let processed: Cow<[Value]> = if stringify.is_empty() {
        Cow::Borrowed(docs)
    } else {
        Cow::Owned(docs.iter().map(|d| stringify_doc(d, &stringify)).collect())
    };

    decode(schema, &processed)
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

/// Decide the Arrow type for one top-level field. Precedence: heterogeneous → epoch → temporal
/// → decimal → base.
fn decide_type(
    name: &str,
    base: &DataType,
    profile: Option<&FieldProfile>,
    opts: &InferenceOptions,
    stringify: &mut HashSet<String>,
) -> DataType {
    if profile.is_some_and(FieldProfile::is_heterogeneous) {
        match opts.heterogeneous {
            HeterogeneousMode::String => {
                stringify.insert(name.to_string());
                return DataType::Utf8;
            }
        }
    }
    if let Some(unit) = opts.epoch_fields.get(name) {
        return DataType::Timestamp(*unit, None);
    }
    if opts.infer_temporal && matches!(base, DataType::Utf8) {
        if let Some(p) = profile {
            if p.all_datetime() {
                return DataType::Timestamp(TimeUnit::Microsecond, None);
            }
            if p.all_date() {
                return DataType::Date32;
            }
        }
    }
    if let NumberMode::Decimal { precision, scale } = opts.number {
        if matches!(base, DataType::Float64) {
            return DataType::Decimal128(precision, scale);
        }
    }
    base.clone()
}

/// Replace the given fields' values with their string form so a `Utf8` decode accepts them.
fn stringify_doc(doc: &Value, fields: &HashSet<String>) -> Value {
    let Value::Object(map) = doc else {
        return doc.clone();
    };
    let mut out = Map::with_capacity(map.len());
    for (k, v) in map {
        if fields.contains(k) {
            out.insert(k.clone(), stringify_value(v));
        } else {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

fn stringify_value(v: &Value) -> Value {
    match v {
        Value::Null | Value::String(_) => v.clone(),
        // numbers → "123", booleans → "true", objects/arrays → compact JSON.
        other => Value::String(other.to_string()),
    }
}

// ── field profiling ─────────────────────────────────────────────────────────

/// The JSON kinds a field takes across the sample (integers and floats both count as
/// `Number`, so a numeric field is never "heterogeneous"), plus temporal-string tallies.
#[derive(Default)]
struct FieldProfile {
    kinds: HashSet<Kind>,
    non_null: usize,
    iso_date: usize,
    iso_datetime: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Bool,
    Number,
    String,
    Object,
    Array,
}

impl FieldProfile {
    fn observe(&mut self, v: &Value) {
        let kind = match v {
            Value::Null => return,
            Value::Bool(_) => Kind::Bool,
            Value::Number(_) => Kind::Number,
            Value::String(s) => {
                if is_iso_datetime(s) {
                    self.iso_datetime += 1;
                } else if is_iso_date(s) {
                    self.iso_date += 1;
                }
                Kind::String
            }
            Value::Object(_) => Kind::Object,
            Value::Array(_) => Kind::Array,
        };
        self.kinds.insert(kind);
        self.non_null += 1;
    }

    fn is_heterogeneous(&self) -> bool {
        self.kinds.len() > 1
    }

    fn all_datetime(&self) -> bool {
        self.non_null > 0 && self.iso_datetime == self.non_null
    }

    fn all_date(&self) -> bool {
        self.non_null > 0 && self.iso_date == self.non_null
    }
}

fn profile_fields(sample: &[Value]) -> HashMap<String, FieldProfile> {
    let mut map: HashMap<String, FieldProfile> = HashMap::new();
    for doc in sample {
        if let Value::Object(obj) = doc {
            for (k, v) in obj {
                map.entry(k.clone()).or_default().observe(v);
            }
        }
    }
    map
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
}
