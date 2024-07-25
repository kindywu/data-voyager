#![allow(unused)]

use std::sync::Arc;

use datafusion::{
    datasource::{provider_as_source, MemTable},
    error::Result,
    execution::context::SessionState,
    functions_aggregate::{
        approx_percentile_cont::approx_percentile_cont,
        count::count,
        expr_fn::{avg, median},
        stddev::stddev,
        sum::sum,
    },
    prelude::{max, min, DataFrame, SessionContext},
};

use datafusion_expr::{case, is_null, lit, try_cast};

use datafusion::logical_expr::{col, LogicalPlanBuilder, UNNAMED_TABLE};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema},
};

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

    for field in original_schema_fields {
        let df = aggregate_all_in_column(df.clone(), field)?;
        let batchs = df.collect().await?;
        let batch = batchs[0].clone();

        let mut array_data = vec![];
        for column in batch.columns() {
            array_data.push(cast(column, &DataType::Float64)?);
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
        avg(try_cast(col(col_name), DataType::Float64)).alias(format!("mean_{0}", col_name)),
        stddev(try_cast(col(col_name), DataType::Float64)).alias(format!("std_{0}", col_name)),
        min(try_cast(col(col_name), DataType::Float64)).alias(format!("min_{0}", col_name)),
        max(try_cast(col(col_name), DataType::Float64)).alias(format!("max_{0}", col_name)),
        median(try_cast(col(col_name), DataType::Float64)).alias(format!("median_{0}", col_name)),
        approx_percentile_cont(try_cast(col(col_name), DataType::Float64), lit(0.25))
            .alias(format!("percentile_25_{0}", col_name)),
        approx_percentile_cont(try_cast(col(col_name), DataType::Float64), lit(0.5))
            .alias(format!("percentile_50_{0}", col_name)),
        approx_percentile_cont(try_cast(col(col_name), DataType::Float64), lit(0.75))
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
