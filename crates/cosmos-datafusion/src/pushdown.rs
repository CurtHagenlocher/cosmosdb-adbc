//! Aggregate pushdown: fold a whole-container `COUNT(*)` / `AVG(col)` (no `GROUP BY`) into a
//! single `SELECT VALUE …` Cosmos round-trip instead of streaming every document into a local
//! DataFusion aggregate.
//!
//! Scope is deliberately tiny and mirrors what the engine executes as a single VALUE aggregate
//! (measured live against the emulator — see the `cosmos-pushdown-surface-empirical` note):
//! exactly one aggregate, no `GROUP BY` / `DISTINCT`, over a bare Cosmos `TableScan` (any WHERE
//! we already push, no `LIMIT`). Anything else stays in DataFusion, which is the correct,
//! reference-validated path.
//!
//! ## Correctness & the two toggles ([`PushdownConfig`])
//!
//! - **`COUNT(*)` → `SELECT VALUE COUNT(1)`** (default **on**). Row-equivalent to DataFusion's
//!   `count(*)`: both count documents/rows regardless of nulls. Only `COUNT(*)` / `COUNT(<lit>)`
//!   is folded — `COUNT(col)` is *not*, because Cosmos `COUNT(c.x)` counts JSON-null values while
//!   DataFusion's `count(col)` skips them.
//! - **`AVG(col)` → `SELECT VALUE AVG(col)`** (default **off**). Cross-partition `AVG` is computed
//!   count-weighted correctly by the engine, but Cosmos null/non-numeric aggregate semantics are
//!   *not* proven equivalent to DataFusion's (which ignore nulls), so pushing it can diverge when
//!   the column holds JSON null / non-numeric values. Opt-in only.
//!
//! This mirrors the Microsoft ODBC driver's two passdown knobs
//! (`EnablePassdownOfAvgAggrFunction`, `EnableSortPassdownForMultipleColumns`): performance/RU
//! toggles that move where work happens. We default `AVG` off (the ODBC driver defaults it on, but
//! it always has a correct local fallback; we would actually push), consistent with this driver's
//! "conservative, provably-equivalent pushdown; richer behavior is opt-in" contract.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use cosmos_client::CosmosClientHandle;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::Transformed;
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::Result;
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan, TableScan, TableType};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::col;

use crate::predicate;
use crate::provider::{CosmosExec, CosmosTableProvider};

/// Which single-aggregate folds the DataFusion dialect performs. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushdownConfig {
    /// Fold `COUNT(*)` into `SELECT VALUE COUNT(1)`. Provably row-equivalent; on by default.
    pub count: bool,
    /// Fold `AVG(col)` into `SELECT VALUE AVG(col)`. Off by default (null-semantics caveat).
    pub avg: bool,
}

impl Default for PushdownConfig {
    fn default() -> Self {
        Self {
            count: true,
            avg: false,
        }
    }
}

/// Optimizer rule that folds a bare single-aggregate over a Cosmos container into one
/// `SELECT VALUE …` scan.
#[derive(Debug)]
pub(crate) struct AggregatePushdown {
    config: PushdownConfig,
}

impl AggregatePushdown {
    pub(crate) fn new(config: PushdownConfig) -> Self {
        Self { config }
    }

    /// Attempt to fold `agg`; `Ok(None)` leaves the aggregate for local execution.
    fn try_fold(&self, agg: &Aggregate) -> Result<Option<LogicalPlan>> {
        // A single bare aggregate, no GROUP BY. Multi-aggregate / grouped queries are rejected
        // by the engine and stay local.
        if !agg.group_expr.is_empty() || agg.aggr_expr.len() != 1 {
            return Ok(None);
        }
        // The input must be a bare Cosmos TableScan. A residual Filter/Projection between the
        // aggregate and the scan (e.g. an unpushable predicate) means this isn't a clean
        // whole-container aggregate — leave it local.
        let LogicalPlan::TableScan(scan) = agg.input.as_ref() else {
            return Ok(None);
        };
        // A fetch on the scan (`… LIMIT n` beneath the aggregate) changes the row set.
        if scan.fetch.is_some() {
            return Ok(None);
        }
        let Some((client, database, container)) = cosmos_parts(scan) else {
            return Ok(None);
        };
        let Some(value_expr) = self.value_expression(&agg.aggr_expr[0]) else {
            return Ok(None);
        };
        // The scan carries only filters we marked `Exact`, so each must translate; bail if not.
        let Some(where_clause) = where_from_filters(&scan.filters) else {
            return Ok(None);
        };

        let mut sql = format!("SELECT VALUE {value_expr} FROM c");
        if let Some(clause) = where_clause {
            sql.push_str(&format!(" WHERE {clause}"));
        }

        // Reproduce the aggregate's single output column exactly (type, nullability, name, no
        // qualifier) so the rewrite is transparent to parent plan nodes.
        let (qualifier, agg_field) = agg.schema.qualified_field(0);
        if qualifier.is_some() {
            return Ok(None);
        }
        let out_name = agg_field.name().to_string();
        let scan_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "value",
            agg_field.data_type().clone(),
            agg_field.is_nullable(),
        )]));

        let provider = Arc::new(CosmosAggregateProvider {
            client,
            database,
            container,
            sql,
            schema: scan_schema,
        });
        let folded = LogicalPlanBuilder::scan("cosmos_agg", provider_as_source(provider), None)?
            .project(vec![col("cosmos_agg.value").alias(out_name)])?
            .build()?;

        // Correctness guard: only accept the rewrite if it reproduces the aggregate's schema
        // (names + types). Any mismatch falls back to the local aggregate.
        if !folded
            .schema()
            .logically_equivalent_names_and_types(&agg.schema)
        {
            return Ok(None);
        }
        Ok(Some(folded))
    }

    /// The Cosmos `VALUE` expression for a foldable aggregate, or `None` to keep it local.
    fn value_expression(&self, expr: &Expr) -> Option<String> {
        let Expr::AggregateFunction(af) = expr else {
            return None;
        };
        let params = &af.params;
        // DISTINCT / FILTER / ordered aggregates are outside the engine's single-VALUE surface.
        if params.distinct || params.filter.is_some() || !params.order_by.is_empty() {
            return None;
        }
        match af.func.name().to_ascii_lowercase().as_str() {
            "count" if self.config.count => match params.args.as_slice() {
                // COUNT(*) is analyzed to COUNT(<non-null literal>); COUNT(col) is excluded
                // (Cosmos counts JSON-null, DataFusion does not).
                [Expr::Literal(sv, _)] if !sv.is_null() => Some("COUNT(1)".to_string()),
                _ => None,
            },
            "avg" if self.config.avg => {
                // DataFusion's coercible AVG signature wraps a non-`Float64` column in a `Cast`
                // (e.g. `avg(CAST(t.n AS Float64))`); peel it to the underlying column. The cast
                // is numeric-only, so `AVG(c["col"])` over the raw JSON numbers is equivalent.
                let [arg] = params.args.as_slice() else {
                    return None;
                };
                let name = as_agg_column(arg)?;
                Some(format!("AVG({})", predicate::cosmos_property(name)))
            }
            _ => None,
        }
    }
}

/// The bare column name behind an aggregate argument, peeling a numeric coercion `Cast` /
/// `TryCast` and any alias. Anything else (a computed expression) yields `None`.
fn as_agg_column(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Alias(a) => as_agg_column(&a.expr),
        Expr::Cast(c) => as_agg_column(&c.expr),
        Expr::TryCast(c) => as_agg_column(&c.expr),
        _ => None,
    }
}

impl OptimizerRule for AggregatePushdown {
    fn name(&self) -> &str {
        "cosmos_aggregate_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        match self.try_fold(agg)? {
            Some(folded) => Ok(Transformed::yes(folded)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

/// The transport handle and target container if `scan` is over a Cosmos table, else `None`.
fn cosmos_parts(scan: &TableScan) -> Option<(Arc<CosmosClientHandle>, String, String)> {
    let provider = source_as_provider(&scan.source).ok()?;
    let cosmos = provider.as_any().downcast_ref::<CosmosTableProvider>()?;
    Some(cosmos.parts())
}

/// Combine already-pushed (Exact) scan filters into a `WHERE` clause. The outer `Option` is
/// `None` if any filter fails to translate (bail); `Some(None)` means no filters.
fn where_from_filters(filters: &[Expr]) -> Option<Option<String>> {
    if filters.is_empty() {
        return Some(None);
    }
    let mut clauses = Vec::with_capacity(filters.len());
    for f in filters {
        clauses.push(predicate::translate(f)?);
    }
    Some(Some(clauses.join(" AND ")))
}

/// A synthetic single-row table backing a folded aggregate: its scan issues the
/// `SELECT VALUE <agg>` query and decodes the scalar into the aggregate's output schema.
struct CosmosAggregateProvider {
    client: Arc<CosmosClientHandle>,
    database: String,
    container: String,
    sql: String,
    schema: SchemaRef,
}

impl fmt::Debug for CosmosAggregateProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CosmosAggregateProvider")
            .field("container", &self.container)
            .field("sql", &self.sql)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CosmosAggregateProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CosmosExec::new_scalar_agg(
            self.client.clone(),
            self.database.clone(),
            self.container.clone(),
            self.sql.clone(),
            self.schema.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use cosmos_client::Credential;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    /// Build a `SessionContext` with a Cosmos-backed table `t` registered and the pushdown rule
    /// installed. The client points at an unreachable endpoint — planning never does I/O.
    fn ctx_with_table(config: PushdownConfig) -> SessionContext {
        let client = Arc::new(
            CosmosClientHandle::connect(
                "https://127.0.0.1:1/",
                Credential::Key(
                    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
                        .into(),
                ),
            )
            .expect("build client"),
        );
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let provider = CosmosTableProvider::new(client, "db".into(), "t".into(), schema);

        let ctx = SessionContext::new();
        ctx.add_optimizer_rule(Arc::new(AggregatePushdown::new(config)));
        ctx.register_table("t", Arc::new(provider))
            .expect("register table");
        ctx
    }

    /// Optimize `sql` and return the pushed Cosmos SQL from the physical plan, if the aggregate
    /// was folded (the folded plan is a `CosmosExec` over a `SELECT VALUE …` query).
    async fn folded_sql(ctx: &SessionContext, sql: &str) -> Option<String> {
        let logical = ctx.state().create_logical_plan(sql).await.expect("plan");
        let optimized = ctx.state().optimize(&logical).expect("optimize");
        let physical = ctx
            .state()
            .create_physical_plan(&optimized)
            .await
            .expect("physical");
        let text = displayable(physical.as_ref()).indent(true).to_string();
        text.lines()
            .find_map(|l| l.split_once("sql=").map(|(_, s)| s.trim().to_string()))
            .filter(|s| s.contains("SELECT VALUE"))
    }

    #[tokio::test]
    async fn count_star_folds() {
        let ctx = ctx_with_table(PushdownConfig::default());
        assert_eq!(
            folded_sql(&ctx, "SELECT COUNT(*) FROM t").await.as_deref(),
            Some("SELECT VALUE COUNT(1) FROM c")
        );
    }

    #[tokio::test]
    async fn count_star_folds_with_where() {
        let ctx = ctx_with_table(PushdownConfig::default());
        let sql = folded_sql(&ctx, "SELECT COUNT(*) FROM t WHERE n > 5").await;
        assert_eq!(
            sql.as_deref(),
            Some(
                r#"SELECT VALUE COUNT(1) FROM c WHERE (IS_DEFINED(c["n"]) AND NOT IS_NULL(c["n"]) AND (c["n"] > 5))"#
            )
        );
    }

    #[tokio::test]
    async fn count_of_column_does_not_fold() {
        // COUNT(col) has different null semantics on Cosmos, so it must stay local.
        let ctx = ctx_with_table(PushdownConfig::default());
        assert_eq!(folded_sql(&ctx, "SELECT COUNT(n) FROM t").await, None);
    }

    #[tokio::test]
    async fn group_by_does_not_fold() {
        let ctx = ctx_with_table(PushdownConfig::default());
        assert_eq!(
            folded_sql(&ctx, "SELECT COUNT(*) FROM t GROUP BY id").await,
            None
        );
    }

    #[tokio::test]
    async fn avg_respects_the_toggle() {
        // Off by default.
        let off = ctx_with_table(PushdownConfig::default());
        assert_eq!(folded_sql(&off, "SELECT AVG(n) FROM t").await, None);

        // On when explicitly enabled.
        let on = ctx_with_table(PushdownConfig {
            count: true,
            avg: true,
        });
        assert_eq!(
            folded_sql(&on, "SELECT AVG(n) FROM t").await.as_deref(),
            Some(r#"SELECT VALUE AVG(c["n"]) FROM c"#)
        );
    }

    #[tokio::test]
    async fn count_toggle_off_keeps_local() {
        let ctx = ctx_with_table(PushdownConfig {
            count: false,
            avg: false,
        });
        assert_eq!(folded_sql(&ctx, "SELECT COUNT(*) FROM t").await, None);
    }
}
