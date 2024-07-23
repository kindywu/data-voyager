mod connect;
mod describe;
mod head;
mod list;
mod schema;
mod sql;

use clap::Parser;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::Result;

pub use self::{
    connect::{connect, ConnectOpts, DatasetConn},
    describe::{describe, DescribeOpts},
    head::{head, HeadOpts},
    list::{list, ListOpts},
    schema::{schema, SchemaOpts},
    sql::{sql, SqlOpts},
};

pub type ReplResult = Result<Option<String>>;

#[enum_dispatch(CmdExector)]
#[derive(Debug, Parser)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Voyager"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List(ListOpts),
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(about = "Show first few rows of a dataset")]
    Head(HeadOpts),
    #[command(name = "schema", about = "Describe the schema of a dataset")]
    Schema(SchemaOpts),
    #[command(about = "Query a dataset using given SQL")]
    Sql(SqlOpts),
}
