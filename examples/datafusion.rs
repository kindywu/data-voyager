use anyhow::Result;
use arrow::array::AsArray;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<()> {
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
