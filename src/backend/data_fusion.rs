use std::ops::Deref;

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::cli::DatasetConn;

use super::{Backend, ReplDisplay};

#[allow(unused)]
pub struct DataFusionBackend(SessionContext);

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;
        let ctx = SessionContext::new_with_config(config);
        Self(ctx)
    }
}

impl Backend for DataFusionBackend {
    type DataFrame = datafusion::dataframe::DataFrame;

    async fn connect(&mut self, opts: &crate::ConnectOpts) -> anyhow::Result<()> {
        match &opts.conn {
            DatasetConn::Postgres(_conn_str) => {
                println!("Postgres connection is not supported yet")
            }
            DatasetConn::Csv(filename) => {
                self.register_csv(&opts.name, filename, Default::default())
                    .await?;
            }
            DatasetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, filename, Default::default())
                    .await?;
            }
            DatasetConn::NdJson(filename) => {
                self.register_json(&opts.name, filename, Default::default())
                    .await?;
            }
        }
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Self::DataFrame> {
        todo!()
    }

    async fn schema(&self, _name: &str) -> anyhow::Result<Self::DataFrame> {
        todo!()
    }

    async fn describe(&self, _name: &str) -> anyhow::Result<Self::DataFrame> {
        todo!()
    }

    async fn head(&self, _name: &str, _size: usize) -> anyhow::Result<Self::DataFrame> {
        todo!()
    }

    async fn sql(&self, _sql: &str) -> anyhow::Result<Self::DataFrame> {
        todo!()
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplDisplay for datafusion::dataframe::DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let batches = self.collect().await?;
        let data = pretty_format_batches(&batches)?;
        Ok(data.to_string())
    }
}
