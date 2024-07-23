use clap::{ArgMatches, Parser};

use crate::{
    backend::{Backend, ReplDisplay},
    CmdExector, ReplContext,
};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct SqlOpts {
    sql: String,
}

impl SqlOpts {
    fn new(sql: String) -> Self {
        Self { sql }
    }
}

pub fn sql(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("sql")
        .expect("expect sql")
        .to_string();

    let cmd = SqlOpts::new(name).into();

    Ok(ctx.send(cmd))
}

impl CmdExector for SqlOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.sql(&self.sql).await?;
        df.display().await
    }
}
