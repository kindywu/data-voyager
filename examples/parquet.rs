use std::fs::File;

use anyhow::{Ok, Result};
use arrow::array::AsArray;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() -> Result<()> {
    let file = "data/user_stats.parquet";
    let file = File::open(file)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .with_limit(3)
        .build()?;

    for record_batch in reader {
        let record_batch = record_batch?;

        let emails = record_batch.column(0).as_string::<i32>();

        for email in emails {
            println!("{:?}", email);
        }
    }
    Ok(())
}
