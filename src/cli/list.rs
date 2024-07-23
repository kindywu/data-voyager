use clap::{ArgMatches, Parser};

use crate::{
    backend::{Backend, ReplDisplay},
    CmdExector, ReplContext,
};

use super::ReplResult;

#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(_args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    ctx.send(ListOpts.into());

    Ok(None)
}

impl CmdExector for ListOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.list().await?;
        df.display().await
    }
}
