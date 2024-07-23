use clap::{ArgMatches, Parser};

use crate::{
    backend::{Backend, ReplDisplay},
    CmdExector, ReplContext,
};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct SchemaOpts {
    name: String,
}

impl SchemaOpts {
    fn new(name: String) -> Self {
        Self { name }
    }
}

pub fn schema(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let cmd = SchemaOpts::new(name).into();

    Ok(ctx.send(cmd))
}

impl CmdExector for SchemaOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.schema(&self.name).await?;
        df.display().await
    }
}
