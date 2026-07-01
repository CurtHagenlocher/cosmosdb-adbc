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
use arrow_schema::{DataType, Field, Schema};
use driverbase::error::ErrorHelper as _;
use serde_json::Value;

use crate::error::ErrorHelper;

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
