use clap::{ArgMatches, Parser};

use crate::{backend::Backend, CmdExector, ReplContext};

use super::ReplResult;

#[allow(unused)]
#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Csv(String, String),
    Parquet(String, String),
    NdJson(String, String),
}

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = verify_conn_str, help = "Connection string to the dataset, could be postgres of local file (support: csv, parquet, json)")]
    pub conn: DatasetConn,
    #[arg(short, long, help = "If database, the name of the table")]
    pub table: Option<String>,
    #[arg(short, long, help = "The name of the dataset")]
    pub name: String,
}

impl ConnectOpts {
    fn new(conn: DatasetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}

pub fn connect(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let conn = args
        .get_one::<DatasetConn>("conn")
        .expect("expect conn_str")
        .to_owned();
    let table = args.get_one::<String>("table").map(|s| s.to_string());
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let cmd = ConnectOpts::new(conn, table, name).into();

    Ok(ctx.send(cmd))
}

fn verify_conn_str(s: &str) -> Result<DatasetConn, String> {
    let conn_str = s.to_string();
    if conn_str.starts_with("postgres://") {
        Ok(DatasetConn::Postgres(conn_str))
    } else if conn_str.ends_with(".csv") {
        Ok(DatasetConn::Csv(conn_str, "csv".to_string()))
    } else if conn_str.ends_with(".parquet") {
        Ok(DatasetConn::Parquet(conn_str, "parquet".to_string()))
    } else if conn_str.ends_with(".ndjson") {
        Ok(DatasetConn::NdJson(conn_str, "ndjson".to_string()))
    } else {
        Err(format!("Invalid connection string: {}", s))
    }
}

impl CmdExector for ConnectOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;
        Ok(format!(
            "Connected to dataset: {} {:?}",
            self.name, self.conn
        ))
    }
}
