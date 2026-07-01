//! The ADBC [`Statement`] object.
//!
//! Carries the per-statement dialect toggle (`native` vs `datafusion`) and the output
//! representation (`json` | `variant` | `struct`). Phase 0 parses/stores these and the
//! SQL text; `execute` is wired up in Phase 1 once the `cosmos-client` transport exists.

use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use cosmos_client::CosmosClientHandle;
use driverbase::error::ErrorHelper as _;

use crate::batch_reader::SingleBatchReader;
use crate::error::ErrorHelper;
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
    query: Option<String>,
}

impl CosmosStatement {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        client: Arc<CosmosClientHandle>,
        database: Option<String>,
    ) -> Self {
        Self {
            runtime,
            client,
            database,
            container: None,
            dialect: Dialect::default(),
            output: OutputMode::default(),
            sample_size: None,
            query: None,
        }
    }
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
                    OutputMode::Variant => {
                        return Err(ErrorHelper::not_implemented()
                            .message("variant output mode (later phase)")
                            .to_adbc());
                    }
                    OutputMode::Struct => {
                        return Err(ErrorHelper::not_implemented()
                            .message("struct output mode (later phase)")
                            .to_adbc());
                    }
                };
                Ok(Box::new(SingleBatchReader::new(batch)))
            }
            Dialect::DataFusion => Err(ErrorHelper::not_implemented()
                .message("datafusion dialect (Phase 2)")
                .to_adbc()),
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
