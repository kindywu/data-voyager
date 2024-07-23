mod data_fusion;

use crate::ConnectOpts;
use anyhow::Result;

pub use data_fusion::DataFusionBackend;

pub trait Backend {
    type DataFrame: ReplDisplay;
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()>;
    async fn list(&self) -> Result<Self::DataFrame>;
    async fn schema(&self, name: &str) -> Result<Self::DataFrame>;
    async fn describe(&self, name: &str) -> Result<Self::DataFrame>;
    async fn head(&self, name: &str, size: usize) -> Result<Self::DataFrame>;
    async fn sql(&self, sql: &str) -> Result<Self::DataFrame>;
}

pub trait ReplDisplay {
    async fn display(self) -> Result<String>;
}
