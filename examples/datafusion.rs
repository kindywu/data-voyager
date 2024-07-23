#![allow(unused)]

use anyhow::Result;
use arrow::{array::AsArray, util::pretty::pretty_format_batches};
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions, SessionContext};

#[tokio::main]
async fn main() -> Result<()> {
    // describe_csv().await?;
    describe_ndjson().await?;
    // sql().await?;
    Ok(())
}

async fn describe_csv() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_csv("data/teams.csv", CsvReadOptions::default())
        .await?;
    let df = df.describe().await?;
    let batches = df.collect().await?;
    let data = pretty_format_batches(&batches)?;
    println!("{data}");
    Ok(())
}

async fn describe_ndjson() -> Result<()> {
    let ctx = SessionContext::new();
    let options = NdJsonReadOptions {
        file_extension: "ndjson",
        ..Default::default()
    };
    let df = ctx.read_json("data/users.ndjson", options).await?;
    let df = df.describe().await?;
    let batches = df.collect().await?;
    let data = pretty_format_batches(&batches)?;
    println!("{data}");
    Ok(())
}

async fn sql() -> Result<()> {
    let file = "data/user_stats.parquet";
    let ctx = SessionContext::new();
    ctx.register_parquet("stats", file, Default::default())
        .await?;

    let ret = ctx
        .sql("SELECT email::text email, name::text name FROM stats limit 3")
        .await?
        .collect()
        .await?;

    println!("{:?}", ret);

    for batch in ret {
        let emails = batch.column(0).as_string::<i32>();
        let names = batch.column(1).as_string::<i32>();

        for (email, name) in emails.iter().zip(names.iter()) {
            let (email, name) = (email.unwrap(), name.unwrap());
            println!("{} {}", email, name);
        }
    }
    Ok(())
}
