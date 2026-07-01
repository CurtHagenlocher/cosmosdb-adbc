//! A minimal single-batch [`RecordBatchReader`], used to return one-shot Arrow results.

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};

/// Yields exactly one [`RecordBatch`], then ends. Handy for metadata results and, later,
/// small single-page query results.
#[allow(dead_code)]
pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    #[allow(dead_code)]
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Streams a fixed list of [`RecordBatch`]es (e.g. the result of a DataFusion collect).
pub struct VecBatchReader {
    schema: SchemaRef,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl VecBatchReader {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
        }
    }
}

impl Iterator for VecBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for VecBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
