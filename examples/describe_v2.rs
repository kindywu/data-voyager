#![allow(unused)]

use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;

use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont;
use datafusion::functions_aggregate::{
    average::avg, count::count, median::median, stddev::stddev, sum::sum,
};
use datafusion::prelude::{CsvReadOptions, DataFrame, SessionContext};
use datafusion_expr::{case, col, is_null, lit, max, min, try_cast, Expr};
use polars::chunked_array::float;
use reedline_repl_rs::yansi::Paint;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_csv("data/teams.csv", CsvReadOptions::default())
        .await?;

    // print_dataframe(df.clone()).await?;

    let df = describe(df).await?;

    print_dataframe(df.clone()).await?;
    Ok(())
}

async fn describe(df: DataFrame) -> Result<DataFrame> {
    let original_schema_fields = df.schema().fields().iter();

    let describe = Field::new("describe", DataType::Utf8, false);
    let mut fields = vec![describe];
    fields.extend(
        original_schema_fields
            .clone()
            .map(|field| Field::new(field.name(), DataType::Float64, true)),
    );

    let schema = Schema::new(fields);

    let describe = StringArray::from(vec![
        "count",
        "null_count",
        "mean",
        "std",
        "min",
        "max",
        "median",
        "percentile_25",
        "percentile_50",
        "percentile_75",
    ]);

    let mut columns: Vec<ArrayRef> = vec![Arc::new(describe)];

    let batches = vec![
        DescribeDataFrame::count(df.clone()),
        DescribeDataFrame::null_count(df.clone()),
        DescribeDataFrame::mean(df.clone()),
        DescribeDataFrame::stddev(df.clone()),
        DescribeDataFrame::min(df.clone()),
        DescribeDataFrame::max(df.clone()),
        DescribeDataFrame::medium(df.clone()),
        DescribeDataFrame::approx_percentile(df.clone(), 0.25),
        DescribeDataFrame::approx_percentile(df.clone(), 0.5),
        DescribeDataFrame::approx_percentile(df.clone(), 0.75),
    ];

    for field in original_schema_fields {
        let mut array_data = vec![];
        for result in batches.iter() {
            let array_ref = match result {
                Ok(df) => {
                    let batchs = df.clone().collect().await;
                    match batchs {
                        Ok(batchs)
                            if batchs.len() == 1
                                && batchs[0].column_by_name(field.name()).is_some() =>
                        {
                            let column = batchs[0].column_by_name(field.name()).unwrap();
                            cast(column, &DataType::Float64)?
                        }
                        _ => Arc::new(Float64Array::from(vec![None])),
                    }
                }
                //Handling error when only boolean/binary column, and in other cases
                Err(err)
                    if err.to_string().contains(
                        "Error during planning: \
                                        Aggregate requires at least one grouping \
                                        or aggregate expression",
                    ) =>
                {
                    Arc::new(StringArray::from(vec!["null"]))
                }
                Err(other_err) => {
                    panic!("{other_err}")
                }
            };
            array_data.push(array_ref);
        }

        columns.push(concat(
            array_data
                .iter()
                .map(|af| af.as_ref())
                .collect::<Vec<_>>()
                .as_slice(),
        )?);
    }

    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
    let df = SessionContext::new().read_batch(batch)?;

    Ok(df)
}

async fn print_dataframe(df: DataFrame) -> Result<()> {
    // print data
    let batches = df.collect().await?;
    let data = pretty_format_batches(&batches)?;
    println!("{data}");
    Ok(())
}

struct DescribeDataFrame;

impl DescribeDataFrame {
    fn count(df: DataFrame) -> Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter();
        let df = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| count(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(df)
    }

    fn null_count(df: DataFrame) -> anyhow::Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter();
        let ret = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| {
                    sum(case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap())
                    .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn mean(df: DataFrame) -> anyhow::Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter();
        let ret = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| avg(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn stddev(df: DataFrame) -> anyhow::Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter();
        let ret = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| stddev(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn min(df: DataFrame) -> anyhow::Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter();
        let ret = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| min(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn max(df: DataFrame) -> anyhow::Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter();
        let ret = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| max(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn medium(df: DataFrame) -> anyhow::Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter();
        let ret = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| median(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn approx_percentile(df: DataFrame, percentile: f64) -> anyhow::Result<DataFrame> {
        let original_schema_fields: std::slice::Iter<Arc<Field>> = df.schema().fields().iter();
        let ret = df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| approx_percentile_cont(col(f.name()), lit(percentile)).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
}
