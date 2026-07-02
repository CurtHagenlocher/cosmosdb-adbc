//! `CosmosTableProvider` (a container as a DataFusion table) and `CosmosExec` (its scan).

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cosmos_client::CosmosClientHandle;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use futures::TryStreamExt;

use crate::{convert, predicate};

/// A single Cosmos container exposed as a DataFusion table with an inferred schema.
pub struct CosmosTableProvider {
    client: Arc<CosmosClientHandle>,
    database: String,
    container: String,
    schema: SchemaRef,
}

impl CosmosTableProvider {
    pub fn new(
        client: Arc<CosmosClientHandle>,
        database: String,
        container: String,
        schema: SchemaRef,
    ) -> Self {
        Self {
            client,
            database,
            container,
            schema,
        }
    }

    /// The transport handle and target `(database, container)` — used by the aggregate
    /// pushdown rule to build a folded scan over the same container.
    pub(crate) fn parts(&self) -> (Arc<CosmosClientHandle>, String, String) {
        (self.client.clone(), self.database.clone(), self.container.clone())
    }
}

impl fmt::Debug for CosmosTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CosmosTableProvider")
            .field("database", &self.database)
            .field("container", &self.container)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CosmosTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Report which filters Cosmos can evaluate exactly. DataFusion only passes the ones we
    /// mark `Exact` to [`scan`](Self::scan); the rest it applies locally. See
    /// [`crate::predicate`] for the (deliberately small, provably row-equivalent) set.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(predicate::pushdown_decisions(filters))
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // DataFusion passes only filters we marked `Exact`; translate each and AND them
        // into a single WHERE clause. `translate` mirrors the pushdown decision, so every
        // filter here is expected to translate.
        let clauses: Vec<String> = filters.iter().filter_map(predicate::translate).collect();
        let where_clause = if clauses.is_empty() {
            None
        } else {
            Some(clauses.join(" AND "))
        };
        let (projected_schema, sql) =
            convert::build_scan_sql(&self.schema, projection, where_clause.as_deref(), limit);
        Ok(Arc::new(CosmosExec::new(
            self.client.clone(),
            self.database.clone(),
            self.container.clone(),
            sql,
            projected_schema,
        )))
    }
}

/// How [`CosmosExec`] turns the query response into Arrow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeMode {
    /// The response is a stream of JSON documents, decoded into the scan schema.
    Docs,
    /// The response is a single `SELECT VALUE <agg>` scalar (aggregate pushdown).
    ScalarAgg,
}

/// Physical scan of one Cosmos container: runs the generated Cosmos SQL through the engine
/// and projects the resulting documents into the scan's Arrow schema. All I/O is deferred to
/// stream-poll time so we never block inside DataFusion's async executor.
pub(crate) struct CosmosExec {
    client: Arc<CosmosClientHandle>,
    database: String,
    container: String,
    sql: String,
    schema: SchemaRef,
    decode: DecodeMode,
    properties: Arc<PlanProperties>,
}

impl CosmosExec {
    pub(crate) fn new(
        client: Arc<CosmosClientHandle>,
        database: String,
        container: String,
        sql: String,
        schema: SchemaRef,
    ) -> Self {
        Self::with_decode(client, database, container, sql, schema, DecodeMode::Docs)
    }

    /// A scan whose query is a single `SELECT VALUE <aggregate>` returning one scalar row.
    pub(crate) fn new_scalar_agg(
        client: Arc<CosmosClientHandle>,
        database: String,
        container: String,
        sql: String,
        schema: SchemaRef,
    ) -> Self {
        Self::with_decode(client, database, container, sql, schema, DecodeMode::ScalarAgg)
    }

    fn with_decode(
        client: Arc<CosmosClientHandle>,
        database: String,
        container: String,
        sql: String,
        schema: SchemaRef,
        decode: DecodeMode,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            client,
            database,
            container,
            sql,
            schema,
            decode,
            properties,
        }
    }
}

impl fmt::Debug for CosmosExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CosmosExec")
            .field("container", &self.container)
            .field("sql", &self.sql)
            .finish()
    }
}

impl DisplayAs for CosmosExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CosmosExec: container={}, sql={}", self.container, self.sql)
    }
}

fn external(message: String) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(message)))
}

impl ExecutionPlan for CosmosExec {
    fn name(&self) -> &str {
        "CosmosExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let client = self.client.clone();
        let database = self.database.clone();
        let container = self.container.clone();
        let sql = self.sql.clone();
        let schema = self.schema.clone();
        let decode = self.decode;
        let out_schema = self.schema.clone();

        let stream = futures::stream::once(async move {
            let docs = client
                .query_documents(&database, &container, &sql)
                .await
                .map_err(|e| external(format!("cosmos query failed: {e}")))?;
            let batches = match decode {
                DecodeMode::Docs => convert::decode_docs(schema, &docs),
                DecodeMode::ScalarAgg => convert::decode_scalar_agg(schema, &docs),
            }
            .map_err(|e| external(format!("decode failed: {e}")))?;
            Ok::<_, DataFusionError>(futures::stream::iter(batches.into_iter().map(Ok)))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
