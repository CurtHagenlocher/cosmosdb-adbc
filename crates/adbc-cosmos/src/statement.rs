//! The ADBC [`Statement`] object.
//!
//! Carries the per-statement dialect toggle (`native` vs `datafusion`) and the output
//! representation (`json` | `variant` | `struct`). Phase 0 parses/stores these and the
//! SQL text; `execute` is wired up in Phase 1 once the `cosmos-client` transport exists.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{Schema, TimeUnit};
use cosmos_client::CosmosClientHandle;
use driverbase::error::ErrorHelper as _;

use crate::batch_reader::{SingleBatchReader, VecBatchReader};
use crate::error::ErrorHelper;
use crate::inference::{HeterogeneousMode, InferenceOptions, NumberMode};
use crate::options;
use crate::output;
use crate::runtime::Runtime;

/// Which SQL dialect `set_sql_query` accepts and how the query is executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    /// Native Cosmos SQL, passed through to the container (ODBC-analog path).
    #[default]
    Native,
    /// ANSI SQL over containers-as-tables, executed via DataFusion federation.
    DataFusion,
}

/// How each returned document is represented in the Arrow result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    /// One JSON string per document in an `arrow.json` extension column (default).
    #[default]
    Json,
    /// One Arrow Variant value per document.
    Variant,
    /// Documents projected into an inferred `Struct` schema.
    Struct,
}

/// State carried by a statement. Most fields are consumed once `execute` is implemented
/// in Phase 1; they are parsed and validated now so the option surface is testable.
#[allow(dead_code)]
pub struct CosmosStatement {
    runtime: Arc<Runtime>,
    client: Arc<CosmosClientHandle>,
    database: Option<String>,
    container: Option<String>,
    dialect: Dialect,
    output: OutputMode,
    sample_size: Option<i64>,
    // struct-mode inference knobs (DESIGN §3.5); see `inference_options`.
    number_decimal: bool,
    decimal_precision: u8,
    decimal_scale: i8,
    infer_temporal: bool,
    epoch_fields: HashMap<String, TimeUnit>,
    heterogeneous: HeterogeneousMode,
    // `datafusion`-dialect pushdown toggles (DESIGN §3.2); see `pushdown_config`.
    pushdown_count: bool,
    pushdown_avg: bool,
    pushdown_sort: bool,
    pushdown_multi_sort: bool,
    /// Shared container-schema cache for the `datafusion` dialect (owned by the connection).
    schema_cache: Arc<cosmos_datafusion::SchemaCache>,
    query: Option<String>,
}

impl CosmosStatement {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        client: Arc<CosmosClientHandle>,
        database: Option<String>,
        schema_cache: Arc<cosmos_datafusion::SchemaCache>,
    ) -> Self {
        Self {
            runtime,
            client,
            database,
            schema_cache,
            container: None,
            dialect: Dialect::default(),
            output: OutputMode::default(),
            sample_size: None,
            number_decimal: false,
            decimal_precision: 38,
            decimal_scale: 9,
            infer_temporal: false,
            epoch_fields: HashMap::new(),
            heterogeneous: HeterogeneousMode::default(),
            pushdown_count: true,
            pushdown_avg: false,
            pushdown_sort: true,
            pushdown_multi_sort: false,
            query: None,
        }
    }

    /// The `datafusion`-dialect pushdown configuration from the parsed toggles.
    fn pushdown_config(&self) -> cosmos_datafusion::PushdownConfig {
        cosmos_datafusion::PushdownConfig {
            count: self.pushdown_count,
            avg: self.pushdown_avg,
            sort: self.pushdown_sort,
            multi_sort: self.pushdown_multi_sort,
        }
    }

    /// Assemble the §3.5 inference options from the parsed statement knobs.
    fn inference_options(&self) -> InferenceOptions {
        InferenceOptions {
            number: if self.number_decimal {
                NumberMode::Decimal {
                    precision: self.decimal_precision,
                    scale: self.decimal_scale,
                }
            } else {
                NumberMode::Float64
            },
            infer_temporal: self.infer_temporal,
            epoch_fields: self.epoch_fields.clone(),
            heterogeneous: self.heterogeneous,
        }
    }
}

/// Map a DataFusion error into an ADBC error with a bit of context.
fn df_error(context: &str, e: datafusion::error::DataFusionError) -> adbc_core::error::Error {
    ErrorHelper::internal(context)
        .message(e.to_string())
        .to_adbc()
}

fn parse_dialect(value: &str) -> Result<Dialect> {
    match value {
        "native" => Ok(Dialect::Native),
        "datafusion" => Ok(Dialect::DataFusion),
        other => Err(ErrorHelper::internal("set_option")
            .format(format_args!(
                "invalid dialect '{other}' (expected 'native' or 'datafusion')"
            ))
            .to_adbc()),
    }
}

fn parse_output(value: &str) -> Result<OutputMode> {
    match value {
        "json" => Ok(OutputMode::Json),
        "variant" => Ok(OutputMode::Variant),
        "struct" => Ok(OutputMode::Struct),
        other => Err(ErrorHelper::internal("set_option")
            .format(format_args!(
                "invalid output '{other}' (expected 'json', 'variant', or 'struct')"
            ))
            .to_adbc()),
    }
}

fn opt_err(msg: std::fmt::Arguments) -> adbc_core::error::Error {
    ErrorHelper::internal("set_option").format(msg).to_adbc()
}

/// `float64` → false, `decimal` → true.
fn parse_number_inference(value: &str) -> Result<bool> {
    match value {
        "float64" => Ok(false),
        "decimal" => Ok(true),
        other => Err(opt_err(format_args!(
            "invalid number_inference '{other}' (expected 'float64' or 'decimal')"
        ))),
    }
}

/// `precision,scale` (e.g. `38,9`); precision 1..=38, scale 0..=precision.
fn parse_decimal(value: &str) -> Result<(u8, i8)> {
    let (p, s) = value.split_once(',').ok_or_else(|| {
        opt_err(format_args!("invalid decimal '{value}' (expected 'precision,scale')"))
    })?;
    let precision: u8 = p
        .trim()
        .parse()
        .map_err(|_| opt_err(format_args!("invalid decimal precision '{p}'")))?;
    let scale: i8 = s
        .trim()
        .parse()
        .map_err(|_| opt_err(format_args!("invalid decimal scale '{s}'")))?;
    if !(1..=38).contains(&precision) || scale < 0 || scale as u8 > precision {
        return Err(opt_err(format_args!(
            "decimal precision/scale out of range: precision 1..=38, 0 <= scale <= precision"
        )));
    }
    Ok((precision, scale))
}

fn parse_heterogeneous(value: &str) -> Result<HeterogeneousMode> {
    match value {
        "string" => Ok(HeterogeneousMode::String),
        #[cfg(feature = "variant")]
        "variant" => Ok(HeterogeneousMode::Variant),
        #[cfg(not(feature = "variant"))]
        "variant" => Err(opt_err(format_args!(
            "heterogeneous='variant' requires building the driver with --features variant"
        ))),
        other => Err(opt_err(format_args!(
            "invalid heterogeneous '{other}' (expected 'string' or 'variant')"
        ))),
    }
}

fn parse_bool_onoff(key: &str, value: &str) -> Result<bool> {
    match value {
        "on" | "true" => Ok(true),
        "off" | "false" => Ok(false),
        other => Err(opt_err(format_args!("invalid {key} '{other}' (expected 'on' or 'off')"))),
    }
}

/// Render a boolean toggle back as its canonical `on`/`off` string for `get_option`.
fn onoff(value: bool) -> String {
    if value { "on".to_string() } else { "off".to_string() }
}

/// Parse `name:s,other:ms` into field → epoch unit.
fn parse_epoch_fields(value: &str) -> Result<HashMap<String, TimeUnit>> {
    let mut map = HashMap::new();
    for entry in value.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (name, unit) = entry.split_once(':').ok_or_else(|| {
            opt_err(format_args!("invalid epoch field '{entry}' (expected 'name:s' or 'name:ms')"))
        })?;
        let unit = match unit.trim() {
            "s" => TimeUnit::Second,
            "ms" => TimeUnit::Millisecond,
            other => {
                return Err(opt_err(format_args!(
                    "invalid epoch unit '{other}' for '{name}' (expected 's' or 'ms')"
                )));
            }
        };
        map.insert(name.trim().to_string(), unit);
    }
    Ok(map)
}

impl Statement for CosmosStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(ErrorHelper::not_implemented().message("bind").to_adbc())
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(ErrorHelper::not_implemented()
            .message("bind_stream")
            .to_adbc())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send>> {
        let query = self.query.as_deref().ok_or_else(|| {
            ErrorHelper::invalid_state()
                .message("no query has been set")
                .to_adbc()
        })?;

        match self.dialect {
            Dialect::Native => {
                let database = self.database.as_deref().ok_or_else(|| {
                    ErrorHelper::invalid_state()
                        .message("no database set (adbc.cosmos.database)")
                        .to_adbc()
                })?;
                let container = self.container.as_deref().ok_or_else(|| {
                    ErrorHelper::invalid_state()
                        .message("native dialect requires a container (adbc.cosmos.container)")
                        .to_adbc()
                })?;

                let docs = self
                    .runtime
                    .block_on(self.client.query_documents(database, container, query))
                    .map_err(|e| ErrorHelper::internal("query").message(e.to_string()).to_adbc())?;

                let batch = match self.output {
                    OutputMode::Json => output::build_json_batch(&docs)?,
                    OutputMode::Struct => {
                        // Default sample size mirrors a "read the first page" heuristic.
                        let sample = self.sample_size.unwrap_or(1000).max(1) as usize;
                        crate::inference::build_struct_batch(&docs, sample, &self.inference_options())?
                    }
                    OutputMode::Variant => output::build_variant_batch(&docs)?,
                };
                Ok(Box::new(SingleBatchReader::new(batch)))
            }
            Dialect::DataFusion => {
                let database = self.database.clone().ok_or_else(|| {
                    ErrorHelper::invalid_state()
                        .message("no database set (adbc.cosmos.database)")
                        .to_adbc()
                })?;
                let sample = self.sample_size.unwrap_or(1000).max(1) as usize;
                let client = self.client.clone();
                let cache = self.schema_cache.clone();
                let pushdown = self.pushdown_config();
                let sql = query.to_string();

                let (schema, batches) = self.runtime.block_on(async move {
                    use datafusion::prelude::SessionContext;

                    let ctx = SessionContext::new();
                    cosmos_datafusion::install_pushdown(&ctx, pushdown);
                    cosmos_datafusion::register_cosmos_schema(&ctx, client, database, sample, cache)
                        .map_err(|e| df_error("register schema", e))?;
                    let df = ctx.sql(&sql).await.map_err(|e| df_error("plan sql", e))?;
                    let schema: arrow_schema::SchemaRef =
                        std::sync::Arc::new(df.schema().as_arrow().clone());
                    let batches = df.collect().await.map_err(|e| df_error("execute", e))?;
                    Ok::<_, adbc_core::error::Error>((schema, batches))
                })?;

                Ok(Box::new(VecBatchReader::new(schema, batches)))
            }
        }
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(ErrorHelper::not_implemented()
            .message("execute_update")
            .to_adbc())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(ErrorHelper::not_implemented()
            .message("execute_schema")
            .to_adbc())
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(ErrorHelper::not_implemented()
            .message("execute_partitions")
            .to_adbc())
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(ErrorHelper::not_implemented()
            .message("get_parameter_schema")
            .to_adbc())
    }

    fn prepare(&mut self) -> Result<()> {
        // No server-side prepare; a no-op keeps clients that always prepare working.
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(ErrorHelper::not_implemented()
            .message("set_substrait_plan")
            .to_adbc())
    }

    fn cancel(&mut self) -> Result<()> {
        Err(ErrorHelper::not_implemented().message("cancel").to_adbc())
    }
}

impl Optionable for CosmosStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionStatement::Other(k) => match k.as_str() {
                options::DIALECT => {
                    self.dialect = parse_dialect(&options::require_string(options::DIALECT, value)?)?;
                }
                options::OUTPUT => {
                    self.output = parse_output(&options::require_string(options::OUTPUT, value)?)?;
                }
                options::CONTAINER => {
                    self.container = Some(options::require_string(options::CONTAINER, value)?);
                }
                options::DATABASE => {
                    self.database = Some(options::require_string(options::DATABASE, value)?);
                }
                options::SAMPLE_SIZE => {
                    self.sample_size = Some(options::require_int(options::SAMPLE_SIZE, value)?);
                }
                options::NUMBER_INFERENCE => {
                    self.number_decimal = parse_number_inference(&options::require_string(
                        options::NUMBER_INFERENCE,
                        value,
                    )?)?;
                }
                options::DECIMAL => {
                    let (p, s) = parse_decimal(&options::require_string(options::DECIMAL, value)?)?;
                    self.decimal_precision = p;
                    self.decimal_scale = s;
                }
                options::INFER_TEMPORAL => {
                    self.infer_temporal = parse_bool_onoff(
                        options::INFER_TEMPORAL,
                        &options::require_string(options::INFER_TEMPORAL, value)?,
                    )?;
                }
                options::EPOCH_FIELDS => {
                    self.epoch_fields =
                        parse_epoch_fields(&options::require_string(options::EPOCH_FIELDS, value)?)?;
                }
                options::HETEROGENEOUS => {
                    self.heterogeneous = parse_heterogeneous(&options::require_string(
                        options::HETEROGENEOUS,
                        value,
                    )?)?;
                }
                options::PUSHDOWN_COUNT => {
                    self.pushdown_count = parse_bool_onoff(
                        options::PUSHDOWN_COUNT,
                        &options::require_string(options::PUSHDOWN_COUNT, value)?,
                    )?;
                }
                options::PUSHDOWN_AVG => {
                    self.pushdown_avg = parse_bool_onoff(
                        options::PUSHDOWN_AVG,
                        &options::require_string(options::PUSHDOWN_AVG, value)?,
                    )?;
                }
                options::PUSHDOWN_SORT => {
                    self.pushdown_sort = parse_bool_onoff(
                        options::PUSHDOWN_SORT,
                        &options::require_string(options::PUSHDOWN_SORT, value)?,
                    )?;
                }
                options::PUSHDOWN_MULTI_SORT => {
                    self.pushdown_multi_sort = parse_bool_onoff(
                        options::PUSHDOWN_MULTI_SORT,
                        &options::require_string(options::PUSHDOWN_MULTI_SORT, value)?,
                    )?;
                }
                _ => return Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
            },
            _ => return Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionStatement::Other(k) => match k.as_str() {
                options::DIALECT => Ok(match self.dialect {
                    Dialect::Native => "native".to_string(),
                    Dialect::DataFusion => "datafusion".to_string(),
                }),
                options::OUTPUT => Ok(match self.output {
                    OutputMode::Json => "json".to_string(),
                    OutputMode::Variant => "variant".to_string(),
                    OutputMode::Struct => "struct".to_string(),
                }),
                options::CONTAINER => self
                    .container
                    .clone()
                    .ok_or_else(|| ErrorHelper::get_unknown_option(&key).to_adbc()),
                options::PUSHDOWN_COUNT => Ok(onoff(self.pushdown_count)),
                options::PUSHDOWN_AVG => Ok(onoff(self.pushdown_avg)),
                options::PUSHDOWN_SORT => Ok(onoff(self.pushdown_sort)),
                options::PUSHDOWN_MULTI_SORT => Ok(onoff(self.pushdown_multi_sort)),
                _ => Err(ErrorHelper::get_unknown_option(&key).to_adbc()),
            },
            _ => Err(ErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match &key {
            OptionStatement::Other(k) if k.as_str() == options::SAMPLE_SIZE => self
                .sample_size
                .ok_or_else(|| ErrorHelper::get_unknown_option(&key).to_adbc()),
            _ => Err(ErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }
}
