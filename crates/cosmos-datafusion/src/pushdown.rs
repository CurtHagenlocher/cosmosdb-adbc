//! Cosmos pushdown: fold work the engine can do server-side out of the local DataFusion plan.
//! Two independent folds, each over a bare Cosmos `TableScan` (any WHERE we already push):
//! whole-container single aggregates (`COUNT(*)` / `AVG`) into one `SELECT VALUE …`, and
//! `ORDER BY` into an engine-side sort. Anything else stays in DataFusion — the correct,
//! reference-validated path.
//!
//! Scope mirrors what the engine executes (measured live against the emulator — see the
//! `cosmos-pushdown-surface-empirical` note): a single VALUE aggregate (no `GROUP BY` /
//! `DISTINCT`), or `ORDER BY` (`OrderBy` / `MultipleOrderBy` features).
//!
//! ## Correctness & the toggles ([`PushdownConfig`])
//!
//! - **`COUNT(*)` → `SELECT VALUE COUNT(1)`** (default **on**). Row-equivalent to DataFusion's
//!   `count(*)`: both count documents/rows regardless of nulls. Only `COUNT(*)` / `COUNT(<lit>)`
//!   is folded — `COUNT(col)` is *not*, because Cosmos `COUNT(c.x)` counts JSON-null values while
//!   DataFusion's `count(col)` skips them.
//! - **`AVG(col)` → `SELECT VALUE AVG(col)`** (default **off**). Cross-partition `AVG` is computed
//!   count-weighted correctly by the engine, but Cosmos null/non-numeric aggregate semantics are
//!   *not* proven equivalent to DataFusion's (which ignore nulls), so pushing it can diverge when
//!   the column holds JSON null / non-numeric values. Opt-in only.
//! - **`ORDER BY col …` → engine-side sort** (single-column default **on**, multi-column default
//!   **off**). Two correctness gates make it row/order-equivalent:
//!   1. *Null placement.* Cosmos orders null/undefined as the **smallest** value (first ASC, last
//!      DESC); DataFusion defaults to nulls-largest. So a key is only pushed when its placement is
//!      nulls-smallest, i.e. `nulls_first == asc` (what SQL `NULLS FIRST` on ASC / `NULLS LAST` on
//!      DESC produce). A default `ORDER BY x` therefore stays local — correctly.
//!   2. *Type.* Only numeric keys (`Int64` / `Float64`) are pushed; string collation and the
//!      stringified-heterogeneous representation are not proven to match Cosmos's type-ordered sort.
//!
//!   Multi-column additionally needs a composite index in production, hence the opt-in.
//!
//! This mirrors the Microsoft ODBC driver's two passdown knobs
//! (`EnablePassdownOfAvgAggrFunction`, `EnableSortPassdownForMultipleColumns`): performance/RU
//! toggles that move where work happens. We default `AVG` off (the ODBC driver defaults it on, but
//! it always has a correct local fallback; we would actually push), consistent with this driver's
//! "conservative, provably-equivalent pushdown; richer behavior is opt-in" contract.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use cosmos_client::CosmosClientHandle;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::Transformed;
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::Result;
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan, Sort, TableScan, TableType};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::col;

use crate::predicate;
use crate::provider::{CosmosExec, CosmosTableProvider, OrderBy};

/// Which folds the DataFusion dialect performs. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushdownConfig {
    /// Fold `COUNT(*)` into `SELECT VALUE COUNT(1)`. Provably row-equivalent; on by default.
    pub count: bool,
    /// Fold `AVG(col)` into `SELECT VALUE AVG(col)`. Off by default (null-semantics caveat).
    pub avg: bool,
    /// Push a single-column `ORDER BY` into the engine. On by default (Cosmos auto-indexes every
    /// scalar path, so a single-property sort needs no extra index).
    pub sort: bool,
    /// Also push a **multi**-column `ORDER BY`. Off by default — production Cosmos requires a
    /// composite index on the sort tuple or the query fails at runtime (the emulator is lenient).
    /// The direct analog of the ODBC driver's `EnableSortPassdownForMultipleColumns`.
    pub multi_sort: bool,
}

impl Default for PushdownConfig {
    fn default() -> Self {
        Self {
            count: true,
            avg: false,
            sort: true,
            multi_sort: false,
        }
    }
}

/// Optimizer rule that folds a bare aggregate or an `ORDER BY` over a Cosmos container into the
/// engine-executed scan.
#[derive(Debug)]
pub(crate) struct CosmosPushdown {
    config: PushdownConfig,
}

impl CosmosPushdown {
    pub(crate) fn new(config: PushdownConfig) -> Self {
        Self { config }
    }

    /// Attempt to fold `agg`; `Ok(None)` leaves the aggregate for local execution.
    fn try_fold_aggregate(&self, agg: &Aggregate) -> Result<Option<LogicalPlan>> {
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

    /// Attempt to push `sort` into the engine; `Ok(None)` leaves it for a local sort.
    fn try_fold_sort(&self, sort: &Sort) -> Result<Option<LogicalPlan>> {
        if !self.config.sort || sort.expr.is_empty() {
            return Ok(None);
        }
        // Multi-column ORDER BY needs a composite index in production (opt-in).
        if sort.expr.len() > 1 && !self.config.multi_sort {
            return Ok(None);
        }
        // The input must be a bare Cosmos TableScan with no LIMIT beneath the sort (a LIMIT there
        // would mean limit-then-sort, which `ORDER BY … LIMIT` does not reproduce).
        let LogicalPlan::TableScan(scan) = sort.input.as_ref() else {
            return Ok(None);
        };
        if scan.fetch.is_some() {
            return Ok(None);
        }
        // Keep the provider Arc alive for the borrow of the downcast reference below.
        let Ok(provider_arc) = source_as_provider(&scan.source) else {
            return Ok(None);
        };
        let Some(cosmos) = provider_arc.as_any().downcast_ref::<CosmosTableProvider>() else {
            return Ok(None);
        };
        let schema = cosmos.schema();

        // Build the ORDER BY clause, gating each key on Cosmos-equivalent ordering.
        let mut keys = Vec::with_capacity(sort.expr.len());
        for s in &sort.expr {
            // Cosmos orders null/undefined as the smallest value → first ASC, last DESC. Only push
            // when DataFusion's placement matches (nulls_first == asc); otherwise sort locally.
            if s.nulls_first != s.asc {
                return Ok(None);
            }
            let Some(name) = sort_column(&s.expr) else {
                return Ok(None);
            };
            // Only numeric columns share an identical total order with Cosmos.
            let Ok(field) = schema.field_with_name(name) else {
                return Ok(None);
            };
            if !matches!(field.data_type(), DataType::Int64 | DataType::Float64) {
                return Ok(None);
            }
            let dir = if s.asc { "ASC" } else { "DESC" };
            keys.push(format!("{} {dir}", predicate::cosmos_property(name)));
        }

        // Substitute a provider that appends the ORDER BY (+ any top-N), reusing the original
        // scan's table name / projection / filters / schema so the node is otherwise identical —
        // removing the local `Sort` is transparent because the scan now returns rows in order.
        let sorted = cosmos.with_order_by(OrderBy {
            clause: keys.join(", "),
            fetch: sort.fetch,
        });
        let mut new_scan = scan.clone();
        new_scan.source = provider_as_source(Arc::new(sorted));
        Ok(Some(LogicalPlan::TableScan(new_scan)))
    }
}

/// The bare column name of a sort key (peeling an alias), or `None` for a computed expression.
fn sort_column(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Alias(a) => sort_column(&a.expr),
        _ => None,
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

impl OptimizerRule for CosmosPushdown {
    fn name(&self) -> &str {
        "cosmos_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let folded = match &plan {
            LogicalPlan::Aggregate(agg) => self.try_fold_aggregate(agg)?,
            LogicalPlan::Sort(sort) => self.try_fold_sort(sort)?,
            _ => None,
        };
        match folded {
            Some(new_plan) => Ok(Transformed::yes(new_plan)),
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
            Field::new("m", DataType::Int64, true),
        ]));
        let provider = CosmosTableProvider::new(client, "db".into(), "t".into(), schema);

        let ctx = SessionContext::new();
        ctx.add_optimizer_rule(Arc::new(CosmosPushdown::new(config)));
        ctx.register_table("t", Arc::new(provider))
            .expect("register table");
        ctx
    }

    /// Optimize `sql` and return the single pushed Cosmos SQL string from the physical plan (the
    /// `sql=` field of the `CosmosExec`), or `None` if there is no `CosmosExec`.
    async fn pushed_sql(ctx: &SessionContext, sql: &str) -> Option<String> {
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
    }

    /// The pushed SQL only when the aggregate folded (a `SELECT VALUE …` query).
    async fn folded_sql(ctx: &SessionContext, sql: &str) -> Option<String> {
        pushed_sql(ctx, sql)
            .await
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
            avg: true,
            ..PushdownConfig::default()
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
            ..PushdownConfig::default()
        });
        assert_eq!(folded_sql(&ctx, "SELECT COUNT(*) FROM t").await, None);
    }

    // --- ORDER BY pushdown ---

    /// The pushed SQL when it carries an engine-side `ORDER BY`, else `None`.
    async fn sorted_sql(ctx: &SessionContext, sql: &str) -> Option<String> {
        pushed_sql(ctx, sql)
            .await
            .filter(|s| s.contains("ORDER BY"))
    }

    #[tokio::test]
    async fn single_numeric_order_by_pushes_when_nulls_smallest() {
        let ctx = ctx_with_table(PushdownConfig::default());
        // NULLS FIRST on ASC = nulls-smallest = Cosmos-equivalent.
        assert_eq!(
            sorted_sql(&ctx, "SELECT id, n FROM t ORDER BY n ASC NULLS FIRST")
                .await
                .as_deref(),
            Some(r#"SELECT c["id"] AS id, c["n"] AS n FROM c ORDER BY c["n"] ASC"#)
        );
        // DESC NULLS LAST is also nulls-smallest.
        assert_eq!(
            sorted_sql(&ctx, "SELECT n FROM t ORDER BY n DESC NULLS LAST")
                .await
                .as_deref(),
            Some(r#"SELECT c["n"] AS n FROM c ORDER BY c["n"] DESC"#)
        );
    }

    #[tokio::test]
    async fn default_null_placement_stays_local() {
        // Plain `ORDER BY n` is nulls-largest in DataFusion but nulls-smallest in Cosmos, so it
        // must not push.
        let ctx = ctx_with_table(PushdownConfig::default());
        assert_eq!(sorted_sql(&ctx, "SELECT n FROM t ORDER BY n").await, None);
    }

    #[tokio::test]
    async fn non_numeric_order_by_stays_local() {
        // String collation is not proven equivalent, so ORDER BY on a Utf8 column stays local.
        let ctx = ctx_with_table(PushdownConfig::default());
        assert_eq!(
            sorted_sql(&ctx, "SELECT id FROM t ORDER BY id ASC NULLS FIRST").await,
            None
        );
    }

    #[tokio::test]
    async fn order_by_with_limit_pushes_top_n() {
        let ctx = ctx_with_table(PushdownConfig::default());
        assert_eq!(
            sorted_sql(&ctx, "SELECT n FROM t ORDER BY n ASC NULLS FIRST LIMIT 3")
                .await
                .as_deref(),
            Some(r#"SELECT c["n"] AS n FROM c ORDER BY c["n"] ASC OFFSET 0 LIMIT 3"#)
        );
    }

    #[tokio::test]
    async fn multi_column_order_by_respects_the_toggle() {
        // Off by default → stays local.
        let off = ctx_with_table(PushdownConfig::default());
        assert_eq!(
            sorted_sql(
                &off,
                "SELECT n, m FROM t ORDER BY n ASC NULLS FIRST, m DESC NULLS LAST"
            )
            .await,
            None
        );
        // On when explicitly enabled.
        let on = ctx_with_table(PushdownConfig {
            multi_sort: true,
            ..PushdownConfig::default()
        });
        assert_eq!(
            sorted_sql(
                &on,
                "SELECT n, m FROM t ORDER BY n ASC NULLS FIRST, m DESC NULLS LAST"
            )
            .await
            .as_deref(),
            Some(r#"SELECT c["n"] AS n, c["m"] AS m FROM c ORDER BY c["n"] ASC, c["m"] DESC"#)
        );
    }

    #[tokio::test]
    async fn sort_toggle_off_keeps_local() {
        let ctx = ctx_with_table(PushdownConfig {
            sort: false,
            ..PushdownConfig::default()
        });
        assert_eq!(
            sorted_sql(&ctx, "SELECT n FROM t ORDER BY n ASC NULLS FIRST").await,
            None
        );
    }
}
