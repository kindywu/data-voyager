use anyhow::Result;
use polars::{prelude::LazyFrame, sql::SQLContext};

fn main() -> Result<()> {
    let file = "data/user_stats.parquet";
    let df = LazyFrame::scan_parquet(file, Default::default())?;
    let mut ctx = SQLContext::new();
    ctx.register("stats", df);
    let df = ctx
        .execute("SELECT email::text, name::text FROM stats")?
        .collect()?;

    println!("{:?}", df);
    Ok(())
}
