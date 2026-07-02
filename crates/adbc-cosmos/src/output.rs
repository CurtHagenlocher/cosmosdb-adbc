//! Builders that turn Cosmos documents into an Arrow `RecordBatch`.
//!
//! Phase 1 implements the default **JSON** mode: one row per document, a single column
//! `document` of Arrow `Utf8` annotated as the canonical `arrow.json` extension type,
//! holding the whole document as JSON with all fields (including Cosmos system fields
//! `_rid`/`_self`/`_etag`/`_attachments`/`_ts`) preserved. Variant and inferred-struct
//! modes arrive later.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use driverbase::error::ErrorHelper as _;
use serde_json::Value;

use crate::error::ErrorHelper;

/// Map an Arrow error into an ADBC error with a bit of context.
#[cfg(feature = "variant")]
fn arrow_err(context: &'static str, e: ArrowError) -> adbc_core::error::Error {
    ErrorHelper::internal(context)
        .message(e.to_string())
        .to_adbc()
}

/// The `ARROW:extension:name` metadata key that marks a field as a canonical extension type.
const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
/// The canonical JSON extension type name (Utf8-backed).
const ARROW_JSON: &str = "arrow.json";

/// Build a single-column `arrow.json` batch from documents (default JSON output mode).
pub fn build_json_batch(docs: &[Value]) -> Result<RecordBatch> {
    let array = StringArray::from_iter_values(docs.iter().map(|doc| doc.to_string()));

    let mut metadata = HashMap::new();
    metadata.insert(EXTENSION_NAME_KEY.to_string(), ARROW_JSON.to_string());
    let field = Field::new("document", DataType::Utf8, false).with_metadata(metadata);
    let schema = Arc::new(Schema::new(vec![field]));

    RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).map_err(|e| {
        ErrorHelper::internal("build_json_batch")
            .message(e.to_string())
            .to_adbc()
    })
}

/// Build a batch by inferring an Arrow `Struct` schema from the first `sample_size`
/// documents and projecting all documents into it (struct output mode).
///
/// Fields absent from a given document become null; fields that appear only *after* the
/// sample are dropped (the documented cost of sampling — default to JSON for fully
/// heterogeneous data).
/// Infer an Arrow `Struct` schema from the first `sample_size` documents. Shared by the
/// `struct` output builder and the connection metadata surface (`get_objects` columns,
/// `get_table_schema`). Empty input yields an empty schema. Returns the raw `ArrowError` so
/// callers can map it into whichever error flavor they need (`driverbase` vs ADBC).
pub fn infer_struct_schema(
    docs: &[Value],
    sample_size: usize,
) -> std::result::Result<Arc<Schema>, ArrowError> {
    if docs.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }
    let sample_n = sample_size.max(1);
    let schema =
        infer_json_schema_from_iterator(docs.iter().take(sample_n).map(Ok::<_, ArrowError>))?;
    Ok(Arc::new(schema))
}

/// Build a single-column Arrow **Variant** batch from documents (variant output mode).
/// Requires the experimental `variant` feature.
#[cfg(not(feature = "variant"))]
pub fn build_variant_batch(_docs: &[Value]) -> Result<RecordBatch> {
    Err(ErrorHelper::not_implemented()
        .message("variant output requires building the driver with --features variant")
        .to_adbc())
}

/// The canonical Arrow Variant extension type name.
#[cfg(feature = "variant")]
const ARROW_VARIANT: &str = "arrow.parquet.variant";

/// Build a single-column Arrow **Variant** batch: each document is encoded as a Variant
/// value (Struct of metadata+value binaries) via `json_to_variant`, in a `document` column
/// annotated with the `arrow.parquet.variant` extension type.
#[cfg(feature = "variant")]
pub fn build_variant_batch(docs: &[Value]) -> Result<RecordBatch> {
    let strings = StringArray::from_iter_values(docs.iter().map(|doc| doc.to_string()));
    let input: ArrayRef = Arc::new(strings);

    let variant_array = parquet_variant_compute::json_to_variant(&input)
        .map_err(|e| arrow_err("json_to_variant", e))?;
    let array: ArrayRef = variant_array.into();

    let mut metadata = HashMap::new();
    metadata.insert(EXTENSION_NAME_KEY.to_string(), ARROW_VARIANT.to_string());
    let field = Field::new("document", array.data_type().clone(), true).with_metadata(metadata);
    let schema = Arc::new(Schema::new(vec![field]));

    RecordBatch::try_new(schema, vec![array]).map_err(|e| arrow_err("build_variant_batch", e))
}
