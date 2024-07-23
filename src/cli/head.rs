use clap::{ArgMatches, Parser};

use crate::{
    backend::{Backend, ReplDisplay},
    CmdExector, ReplContext,
};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    name: String,
    size: usize,
}

impl HeadOpts {
    fn new(name: String, size: usize) -> Self {
        Self { name, size }
    }
}

pub fn head(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();
    let size = *args.get_one::<usize>("size").expect("expect size");

    let cmd = HeadOpts::new(name, size).into();
    ctx.send(cmd);

    Ok(None)
}

impl CmdExector for HeadOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.head(&self.name, self.size).await?;
        df.display().await
    }
}
