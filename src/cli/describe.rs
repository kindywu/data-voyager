use clap::{ArgMatches, Parser};

use crate::{
    backend::{Backend, ReplDisplay},
    CmdExector, ReplContext,
};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    name: String,
}

impl DescribeOpts {
    fn new(name: String) -> Self {
        Self { name }
    }
}

pub fn describe(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let cmd = DescribeOpts::new(name).into();

    Ok(ctx.send(cmd))
}

impl CmdExector for DescribeOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(&self.name).await?;
        df.display().await
    }
}
