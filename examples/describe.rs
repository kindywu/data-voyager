#![allow(unused)]

use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Float32Array, Int32Array, RecordBatch, StringArray};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;

use datafusion::config::CsvOptions;
use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont;
use datafusion::functions_aggregate::{
    average::avg, count::count, median::median, stddev::stddev, sum::sum,
};
use datafusion::logical_expr::{case, cast, col, is_null, lit, max, min, try_cast};
use datafusion::prelude::{to_timestamp, CsvReadOptions, DataFrame, SessionContext};
// use datafusion_expr::{case, cast, col, is_null, lit, max, min, try_cast};
use reedline_repl_rs::yansi::Paint;

use datafusion::{functions::expr_fn::length, functions_array::length::array_length};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let original = ctx
        .read_csv("data/teams.csv", CsvReadOptions::new())
        .await?;

    let df = transform(original.clone())?;

    print_dataframe(df.clone()).await?;

    let df = describe(df).await?;

    print_dataframe(df.clone()).await?;

    let df = cast_back(df, original)?;

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
            .map(|field| Field::new(field.name(), DataType::Int64, true)),
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

    for field in original_schema_fields {
        let df = aggregate_all_in_column(df.clone(), field)?;
        let batchs = df.collect().await?;
        let batch = batchs[0].clone();

        let mut array_data = vec![];
        for column in batch.columns() {
            array_data.push(arrow::compute::cast(column, &DataType::Int64)?);
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

fn transform(df: DataFrame) -> Result<DataFrame> {
    let fields = df.schema().fields().iter();
    // change all temporal columns to Float64
    let expressions = fields
        .map(|field| {
            let dt = field.data_type();
            let expr = match dt {
                dt if dt.is_temporal() => cast(col(field.name()), DataType::Int64),
                dt if dt.is_numeric() => col(field.name()),
                DataType::List(_) | DataType::LargeList(_) => array_length(col(field.name())),
                _ => length(cast(col(field.name()), DataType::Utf8)),
            };
            expr.alias(field.name())
        })
        .collect();

    let df = df.select(expressions)?;
    Ok(df)
}

fn aggregate_all_in_column(df: DataFrame, field: &Arc<Field>) -> Result<DataFrame> {
    let col_name = field.name();
    let is_numeric = field.data_type().is_numeric();

    let aggr_expr = vec![
        count(col(col_name)).alias(format!("count_{0}", col_name)),
        sum(case(is_null(col(col_name)))
            .when(lit(true), lit(1))
            .otherwise(lit(0))
            .unwrap())
        .alias(format!("null_count_{0}", col_name)),
        avg(try_cast(col(col_name), DataType::Int32)).alias(format!("mean_{0}", col_name)),
        stddev(try_cast(col(col_name), DataType::Int32)).alias(format!("std_{0}", col_name)),
        min(try_cast(col(col_name), DataType::Int32)).alias(format!("min_{0}", col_name)),
        max(try_cast(col(col_name), DataType::Int32)).alias(format!("max_{0}", col_name)),
        median(try_cast(col(col_name), DataType::Int32)).alias(format!("median_{0}", col_name)),
        approx_percentile_cont(try_cast(col(col_name), DataType::Int32), lit(0.25))
            .alias(format!("percentile_25_{0}", col_name)),
        approx_percentile_cont(try_cast(col(col_name), DataType::Int32), lit(0.5))
            .alias(format!("percentile_50_{0}", col_name)),
        approx_percentile_cont(try_cast(col(col_name), DataType::Int32), lit(0.75))
            .alias(format!("percentile_75_{0}", col_name)),
    ];

    // // print the expr
    // println!(
    //     "col={}, is_numeric={}, {:#?}",
    //     col_name, is_numeric, aggr_expr
    // );

    let df = df.clone().aggregate(vec![], aggr_expr)?;

    Ok(df)
}

fn cast_back(df: DataFrame, original: DataFrame) -> anyhow::Result<DataFrame> {
    // we need the describe column
    let describe = Arc::new(Field::new("describe", DataType::Utf8, false));
    let mut fields = vec![&describe];
    fields.extend(original.schema().fields().iter());
    let expressions = fields
        .into_iter()
        .map(|field| {
            let dt = field.data_type();
            let expr = match dt {
                dt if dt.is_temporal() => cast(col(field.name()), dt.clone()),
                DataType::List(_) | DataType::LargeList(_) => {
                    cast(col(field.name()), DataType::Int32)
                }
                _ => col(field.name()),
            };
            expr.alias(field.name())
        })
        .collect();

    Ok(df.select(expressions)?)
    // .sort(vec![col("describe").sort(true, false)])?)
}

async fn print_dataframe(df: DataFrame) -> Result<()> {
    // print data
    let batches = df.collect().await?;
    let data = pretty_format_batches(&batches)?;
    println!("{data}");
    Ok(())
}
