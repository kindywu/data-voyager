use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};

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

    async fn connect(&mut self, _opts: &crate::ConnectOpts) -> anyhow::Result<()> {
        todo!()
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

impl ReplDisplay for datafusion::dataframe::DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let batches = self.collect().await?;
        let data = pretty_format_batches(&batches)?;
        Ok(data.to_string())
    }
}
